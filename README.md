# Kayrock

[![Build Status](https://travis-ci.org/dantswain/kayrock.svg?branch=master)](https://travis-ci.org/dantswain/kayrock)
[![Coverage Status](https://coveralls.io/repos/github/dantswain/kayrock/badge.svg?branch=master)](https://coveralls.io/github/dantswain/kayrock?branch=master)
[![Module Version](https://img.shields.io/hexpm/v/kayrock.svg)](https://hex.pm/packages/kayrock)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/kayrock/)
[![Total Download](https://img.shields.io/hexpm/dt/kayrock.svg)](https://hex.pm/packages/kayrock)
[![License](https://img.shields.io/hexpm/l/kayrock.svg)](https://hex.pm/packages/kayrock)
[![Last Updated](https://img.shields.io/github/last-commit/dantswain/kayrock.svg)](https://github.com/dantswain/kayrock/commits/master)


Aims to provide an idiomatic Elixir interface to the Kafka protocol.

This work is based on [https://github.com/klarna/kafka_protocol](kafka_protocl)
for Erlang, which is used in [https://github.com/klarna/brod](brod).  It is
built to work with [https://github.com/kafkaex/kafka_ex](KafkaEx) though there
is no reason why it couldn't be used elsewhere.

## Basic architecture and usage

Kayrock generates structs for every version of every message in the Kafka
protocol.  It does this by converting the Erlang-based schema descriptions in
`:kpro_schema` from the `kafka_protocol` library and is therefore limited to
messages and versions included there.  Each request message has a serializer
(that generates iodata), and each response message has a deserializer. There is
also a protocol, `Kayrock.Request`, that has an implementation created for each
request.  The `Kayrock.Request` protocol defines a serializer as well as a
response deserializer factory.

This repo includes a mix task, `mix gen.kafka_protocol`, that produces the code
in `lib/generated`.  I chose to check the generated code into the repository to
simplify its use.  End users should not need to run the mix task unless they are
doing development on Kayrock itself.

The generated structs are namespaced as follows: `Kayrock.<api>.V<version
number>.[Request|Response]`.  For example, the version 4 fetch api corresponds
to `Kayrock.Fetch.V4.Request` and `Kayrock.Fetch.V4.Response`.

An example of basic usage, taken (and modified) from
`Kayrock.broker_sync_call/2`:

```elixir
alias Kayrock.Request

request = %Kayrock.ApiVersions.V1.Request{client_id: "some_client", correlation_id: 0}

# wire_protocol will be iodata representing the serialized request
wire_protocol = Request.serialize(request)

# implementation-specific network request/response handling
:ok = BrokerConnection.send(broker_pid, wire_protocol)
{:ok, resp} = BrokerConnection.recv(broker_pid)

# response_deserializer will be a function that accepts binary data and
# returns {%Kayrock.ApiVersions.V1.Response{}, rest} where rest is any
# extra binary data in the response
response_deserializer = Request.response_deserializer(request)
{deserialized_resp, _} = response_deserializer.(resp)

# deserialized_resp will be a %Kayrock.ApiVersions.V1.Response{} representing
# the api versions supported by the broker
```

## Relationship to other libraries

Kayrock uses only the `kpro_schema` module from `kafka_protocol`.
`kafka_protocol` provides quite a lot of functionality (especially when used as
part of `brod`).  This repo chooses to limit the integration surface between
itself and `kafka_protocol` because

1. `kafka_protocol` produces Erlang records whereas Kayrock aims to provide
   Elixir structs.
2. `kafka_protocol` provides network-level implementation whereas Kayrock is
   intended to only provide serialization and deserialization of the protocol.
3. `kpro_schema` is itself automatically generated from the Java source code of
   Kafka, meaning that Kayrock could feasibly reproduce this without needing an
   intermediate dependency.

This is in no way meant to detract from the great work that the
`kafka_protocol`/`brod` team has done.  I have simply chosen a different path
here.

You will notice that in this repo there is also a lightweight implementation of
a Kafka client.  This serves a few purposes:

1. To help with the development of the Kayrock protocol API.
2. To allow for testing against a server without creating a circular dependency
   on KafkaEx.
3. Because I want to mess around with it.

Regardless, it is not a production-ready implementation. I would refer you to
KafkaEx or brod for that.
