# 0009: No Authentication or Authorization in TRAM

- **Status:** Accepted

## Context

TRAM opens ports, receives data and sends it anywhere a stream says. Authentication, TLS and tenant identity could be built into TRAM, but every deployment already has a way to do them, e.g. an ingress, a service mesh or network policies, and they differ between deployments.

## Decision

TRAM has no authentication or authorization of its own, and the OTLP source serves without TLS. They are the job of the infrastructure in front of TRAM. Access to a configuration source (API, configuration directory, `Stream` resources) is access to the TRAM process. What a proxy knows about a request, e.g. the tenant, reaches the data through `header_attributes`: the header decides the attribute, and a value the client set itself is replaced.

## Consequences

- TRAM has to be deployed behind something that authenticates, and only that may reach its ports; the README's Security section lists what to restrict
- Granting `create` on `Stream` resources is as powerful as access to the TRAM deployment
- Revisit before 1.0 whether TLS on the OTLP source or authentication of the API belongs into TRAM as a generic capability
