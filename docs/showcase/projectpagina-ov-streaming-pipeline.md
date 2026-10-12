# Real-time OV streaming pipeline

**Elk voertuig in het Nederlandse openbaar vervoer, live gestreamd — van GTFS-realtime feed tot een kaart van het hele land, met de correctheidsgaranties erbij.**

---

## Hero / intro

Twee bestanden op een open endpoint bevatten elke minuut waar ongeveer 4.900 voertuigen zich bevinden en hoeveel vertraging ze hebben. Daar een dashboard van maken is een middag werk. Daar een pipeline van maken die 24 uur per dag doorloopt, na een crash de juiste cijfers geeft, kapotte berichten niet stilzwijgend verliest en waarvan je een uur uit het verleden opnieuw kunt uitrekenen — dat is een ander project.

Dit is dat tweede project. Een producer die snapshots in wijzigingsevents omzet, Redpanda als bus, een streamprocessor met tumbling windows en per-partitie watermarks, een ruw archief in MinIO waaruit je kunt replayen, Postgres als serving-laag en een live kaart erbovenop. Dertien containers, `make up`, geen accounts en geen sleutels.

*Korte feiten voor onder de intro:* Python 3.12 · Kafka (Redpanda) · Postgres · MinIO · Prometheus + Grafana · Streamlit + pydeck · Docker Compose — 106 unit tests, ruff schoon — draait volledig lokaal, nul kosten.

---

## Waarom dit project

Streaming leer je niet van een tutorial met een `for`-lus over een lijst. De interessante dingen gebeuren pas als er echte tijd in het spel is: events die te laat aankomen, partities die uit de pas lopen, een proces dat omvalt tussen twee schrijfacties, een feed die zich niet aan zijn eigen documentatie houdt.

Ik wilde die dingen tegenkomen in plaats van erover lezen. Een landelijke, publieke, altijd-aan databron dwingt dat af — en levert meteen iets op wat je kunt laten zien.

---

## Architectuur

> **[DIAGRAM 1 — ov-1-architectuur]**

De keten loopt van links naar rechts. De producer haalt twee protobuf-feeds op, parseert ze naar een geversioneerd eventcontract en publiceert naar twee Kafka-topics. Twee onafhankelijke consumergroepen lezen mee: de processor rekent vensters uit en schrijft naar Postgres, de archiver schrijft de ruwe bytes naar MinIO. Een sidecar laadt dagelijks de statische GTFS-referentiedata. Het dashboard staat op Postgres, Prometheus scrapet alles.

Twee architectuurkeuzes bepalen de rest van het ontwerp.

**De broker is een buffer, geen archief.** De retentie op de ruwe topics staat op 24 uur. Wat je over een maand nog nodig hebt staat in MinIO, als bytes — niet als geparseerde events. Dat onderscheid lijkt klein en is het niet: omdat het archief bytes bewaart, kun je er later een níeuwe decoder overheen draaien. Had je geparseerde events opgeslagen, dan had je de interpretatie van toen voor altijd vastgelegd.

**Twee consumergroepen in plaats van één service die allebei doet.** Archiveren en verwerken hebben verschillende faalmodi en verschillende snelheden. Als MinIO even weg is, moet de processor gewoon doorlopen — en andersom.

---

## Het probleem dat de producer oplost

Beide feeds zijn `FULL_DATASET`-snapshots: elke minuut komt het volledige beeld opnieuw langs. Publiceer je dat rechtstreeks, dan zit je op zo'n 700 berichten per seconde die grotendeels herhaling zijn.

De producer diffed daarom elke snapshot tegen de vorige en publiceert alleen wat veranderd is. In de ochtendspits onderdrukt dat ongeveer 45% van de entiteiten per poll. Om te voorkomen dat een trein die al twintig minuten op +5:00 staat uit de statistiek verdwijnt, gaat er elke zestig seconden een heartbeat mee. Het resultaat: vijf tot twintig keer minder berichten, met identieke venstercijfers.

Daarnaast zit er een verzameling correcties in die rechtstreeks uit de feed komt en die je nergens gedocumenteerd vindt. Entiteits-id's zoals `2026-09-06:GVB:13:13605` bevatten de vervoerderscode die verder nergens in het bericht staat, dus die wordt uit het id geparseerd. `VehiclePosition` bevat geen vertraging — die zit in `TripUpdate`. Er rijden spookvoertuigen rond: een bus die elf dagen op dezelfde plek "STOPPED_AT" staat, en één voertuig dat vanuit 29 minuten in de toekomst rapporteerde. Posities ouder dan tien minuten of meer dan een minuut vooruit worden weggegooid.

Al die gevallen zijn vastgelegd in opgenomen protobuf-fixtures in de repo. 140 KB die elke parsertest aan de werkelijkheid vastpint.

---

## Vensters, watermarks en late events

> **[DIAGRAM 2 — ov-2-vensters-watermarks]**

De processor rekent tumbling vensters van vijf minuten uit, uitgelijnd op het epoch, over vier dimensies tegelijk: lijn, station, vervoerder en netwerk. Per venster: count, gemiddelde, exacte P90, max en min.

De watermark is waar het echte werk zit. Eerste versie: één globaal maximum over alle binnengekomen events. Dat werkte prima tot er een inhaalslag van veertig minuten kwam — de zes partities werden op verschillende snelheden geconsumeerd, de snelste trok de watermark vooruit, en 169.000 volstrekt normale events van de tragere partities werden als "te laat" bestempeld en weggegooid.

De oplossing is wat Flink doet: een watermark per partitie, en het minimum daarvan geldt. Met één toevoeging, want anders houdt een partitie die niets meer stuurt de watermark voor altijd tegen: na 120 seconden stilte telt een partitie niet meer mee tot ze weer iets zegt.

Events achter de watermark worden geteld en weggegooid. Dat is een expliciete afweging: begrensde state en precies één emissie per venster, in ruil voor het negeren van alles wat meer dan twee minuten achterloopt. In de praktijk is dat aantal nul, omdat de event-tijd de generatietijd van de feed zelf is en die monotoon oploopt.

En dan is er nog de nacht. Tussen 01:00 en 05:00 rijden er twintig à dertig nachtbussen in het hele land. Zonder ingreep sluit het laatste venster nooit, want er komt geen event meer dat de watermark vooruit duwt. Na negentig seconden stilte schuift de processor daarom de watermark op de wandklok vooruit.

---

## At-least-once, veilig gemaakt door idempotentie

> **[DIAGRAM 3 — ov-3-at-least-once]**

Exactly-once levering wordt bewust niet nagestreefd. De goedkopere garantie geeft hetzelfde antwoord, zonder transactionele machinerie.

De volgorde is: verwerk, upsert de gefinaliseerde vensters, en commit de offsets pas daarna. Valt het proces om tussen die twee stappen, dan levert de broker dezelfde berichten opnieuw, herberekent de processor hetzelfde venster uit dezelfde events, en schrijft die weg op dezelfde sleutel. De primary key van `delay_aggregates` is `(window_start, dimension, dimension_id)` — de upsert vervángt de rij met identieke waarden in plaats van erbij op te tellen. Dubbeltellen kan dus niet.

Twee details maken dat waterdicht in plaats van ongeveer goed.

**Offsets lopen nooit vóór op open vensters.** Per partitie houdt de processor de geconsumeerde `(offset, window_end)`-paren in aankomstvolgorde bij, en commit één voorbij het langste aaneengesloten begin waarvan alle vensters gesloten zijn. Zit er halverwege een bericht dat nog in een open venster valt, dan stopt de commit daar — ook als er achter dat bericht al wél afgeronde vensters staan. Alles in een open venster wordt na een crash opnieuw geleverd en bouwt dat venster van voren af aan opnieuw op.

**De watermark wordt bij het opstarten uit Postgres gezaaid.** `max(window_end)` zorgt dat opnieuw geleverde berichten die bij een al weggeschreven venster horen als te laat gelden, in plaats van dat er een gedeeltelijk venster over een compleet venster heen wordt geschreven. Die seed wordt op de wandklok geklemd — een les uit de praktijk: één keer zaaide een replay die nog liep de watermark in de toekomst, waarna 130.000 goede events werden weggegooid.

Er is een test die het bewijst tegen de échte database: `test_duplicate_delivery_does_not_double_count_in_real_postgres` past hetzelfde gefinaliseerde venster twee keer toe en controleert dat er één rij staat, met de oorspronkelijke count.

---

## Als het misgaat

> **[DIAGRAM 4 — ov-4-dlq-en-replay]**

Alles wat niet geparseerd (producer) of niet gedecodeerd (processor) kan worden gaat naar `dlq.<source_topic>` als de originele bytes, met headers voor stage, brontopic, partitie, offset, fout, tijdstip en schemaversie. De hoofdlus blokkeert nooit.

Eén detail dat je pas ontdekt als je het draait: parse-fouten herhalen zich elke minuut, want de feed stuurt dezelfde kapotte entiteit gewoon opnieuw. Zonder maatregel loopt je DLQ vol met duizend keer hetzelfde bericht. De producer dead-lettert daarom elke unieke combinatie van entiteit en reden één keer.

Van daaruit lopen twee herstelpaden. `make replay-dlq` draait de mislukte stap opnieuw met de huidige code — een parserfix, een nieuwe schemaversie — en injecteert de geslaagde terug in het brontopic met een `replayed_from`-header; wat nog steeds stuk is blijft staan en wordt samengevat per reden. En `make replay FROM=… TO=…` herberekent een tijdvak uit het MinIO-archief.

Dat laatste heeft één eigenschap waar ik bewust op gestuurd heb: replay draait door dezelfde `Pipeline`-klasse als de live consumer. Geen tweede implementatie van de windowinglogica. Zou dat wel zo zijn, dan test je twee dingen en vertrouw je er één. Gemeten: 122.364 gearchiveerde berichten uit twaalf minuten ochtendspits, herberekend tot 11.323 vensters in ongeveer tien seconden.

---

## Schema-evolutie

`SCHEMA_VERSION` staat zowel in de body van elk bericht als in een Kafka-header, zodat een consument op versie kan routeren zonder de body te parseren. Achterwaarts compatibele wijzigingen — een optioneel veld met een default erbij — bumpen de versie niet. Een veld weghalen, hernoemen of van type veranderen wel.

`common.serde` houdt een decoder per ondersteunde versie bij, en de processor accepteert de huidige én de vorige. Daardoor kunnen producer en consumer onafhankelijk uitgerold worden: eerst de processor die v1 en v2 leest, dan de producer die v2 schrijft, en na 24 uur retentie zonder v1 in omloop kan de oude decoder weg. Een onbekende versie is een `SchemaError` — dus de DLQ, nooit een crash.

En omdat het archief bytes bewaart en geen events, kan een nieuwe decoder altijd over de historie heen worden gedraaid.

---

## Wat het in de praktijk doet

Gemeten op een MacBook Pro uit 2019 met de hele stack in Docker Desktop, landelijke feed, geen filter op vervoerder. `make throughput` haalt deze cijfers rechtstreeks uit Prometheus.

| Maandagochtendspits 08:15–09:15 | |
| --- | --- |
| Berichten in, gemiddeld | 176 per seconde (piekminuut 347) |
| Voertuigen in de feed per poll | 3.625; de snapshot-differ onderdrukte 45% |
| Vensters gefinaliseerd | 2.739 per vijf minuten, 6.994 tegelijk open |
| Consumer lag | p50 nul; maximaal 3.806 direct na een herstart, binnen een minuut ingelopen |
| Watermark lag | 170 seconden — 120 s toegestane vertraging plus ~50 s feedcadans |
| Batchverwerking p95 | 0,18 s voor een batch van 2.000 berichten, inclusief de state-upserts |
| Ruw archief | 718 objecten, 30 MB gzip per uur in de spits; ~1 MB/uur 's nachts |

En twee dingen die je alleen leert door het echt een nacht te laten draaien. De ochtendspits en de nacht schelen een factor honderd: om half twaalf 's avonds zijn het 50 tot 110 berichten per seconde en 1.400 voertuigen, om vier uur 's nachts twee berichten per seconde en twintig bussen — het dashboard zegt dat dan ook eerlijk in plaats van beweging te faken. En een slapende laptop is geen pipeline-storing, maar ziet er in Grafana precies zo uit: macOS wordt elk uur 45 seconden wakker voor onderhoud, wat één hoge piek per uur en een watermark lag van een half uur oplevert. `make keep-awake` staat inmiddels in de runbook.

---

## Ontwerpkeuzes, kort

Een paar afwegingen waar ik het langst over heb nagedacht:

**Pure-Python windowingengine, zonder Kafka-imports.** De vensterlogica kent geen broker. Daardoor zijn de tests deterministisch met synthetische reeksen — inclusief een venster dat met de hand is uitgerekend — en draait dezelfde engine ook de replay.

**Exacte P90 in plaats van een t-digest.** Een paar duizend waarnemingen per venster per sleutel maakt nearest-rank gratis, en het maakt een handberekende test mogelijk. Op echte schaal is dit het onderdeel dat je zou vervangen; de accumulator-interface verandert daar niet van.

**Extra dimensies in plaats van afleiden.** Netwerk-P90 kun je niet uit de P90's per lijn afleiden. Vier dimensies kosten één dict-insert elk, dus die worden gewoon apart bijgehouden.

**Statische GTFS via `COPY` en een atomische tabelwissel.** De zip van 220 MB gaat rechtstreeks in `COPY`, in stagingtabellen die daarna in één transactie worden omgewisseld. Lezers zien nooit een half geladen set. Een dagelijkse `sleep`-loop, geen Airflow — dit is een cron-sidecar, geen orkestratie.

**Python in plaats van Spark Structured Streaming.** Het simpelste dat windowing correct demonstreert. De overstap is een gedocumenteerde migratie, geen voorwaarde vooraf.

---

## Testen en observability

`make test` draait 106 unit tests zonder enige infrastructuur: de parser tegen opgenomen feeds — inclusief een fixture van gemengde kwaliteit en een met pure onzin — serde-rondgangen en elk `SchemaError`-pad, de snapshot-differ, vensterindeling, lateness, duplicaten, per-partitie watermarks, het handberekende venster, idempotentie van de sink, DLQ-headers, de offset-ledger en de SQL-invarianten van het dashboard.

`make test-integration` draait tegen de draaiende stack: de dubbele-levering-test op echte Postgres, de atomische swap van de GTFS-loader, de dashboardqueries. In CI draaien lint en unit tests; de integratietests blijven lokaal.

Prometheus scrapet elke service en Redpanda zelf. Grafana krijgt een geprovisioneerd dashboard van 26 panelen mee — throughput, consumer lag, watermark lag, DLQ-rate, late events, verwerkingstijd, state size en archiefvolume. `make inject-bad` publiceert twee vergiftigde berichten; binnen tien seconden zie je ze in `make dlq-peek`, op het Grafana-paneel en in de health-strip van het dashboard.

---

## Wat dit project laat zien

Kafka en event-driven ontwerp · streamwindowing met watermarks en lateness · at-least-once met idempotente sinks · dead-letter queues en replay · schema-evolutie · Postgres · Docker Compose · Prometheus en Grafana · Streamlit en pydeck.

Of korter: een pipeline waarvan ik kan uitleggen wat er gebeurt als hij omvalt.

---

## Links

- Repository: github.com/datavakwerk/ov-streaming-pipeline
- Databron: OVapi GTFS-realtime, open access zonder sleutel
