# Apache Kafka message specs 4.4.0

These files are unmodified copies of Apache Kafka's protocol message
definitions, licensed under the Apache License, Version 2.0 (see the header
of each file and `NOTICE`).

- Source: https://github.com/apache/kafka.git
- Tag: `4.4.0`
- Commit: `1156b2752aaa28eafc0b4c4d387e63356793268d`
- Path: `clients/src/main/resources/common/message`
- Files: every `*Request.json`, `*Response.json` and `*Header.json`, plus the
  upstream `README.md` describing the format.

Regenerate with:

    mix kayrock.fetch_kafka_messages 4.4.0

Kayrock reads these with `Kayrock.Generate.JsonSpec`; see
`Kayrock.Generate.ApiTable` for which APIs and versions are generated from them.
