# Individual Reflection and Team Contributions

## Project

**Distributed Inventory Management System**

This project implements an inventory-management application using Python,
gRPC, SQLite, FastAPI, and a locally hosted Ollama model. The system supports
authentication, inventory viewing, order processing, historical-sales-based
demand forecasts, reorder recommendations, and AI-assisted analytics.


## Team member contributions

| Team member | Student ID | Primary contribution | Key work and learning |
| --- | --- | --- | --- |
| B Aditya Pai | 2026H1030077P | Frontend, backend, and LLM integration *(proposed)* | Contributed across the browser-facing user experience, service-side functionality, and local-AI workflow. This includes presenting inventory and analytics clearly, supporting API and business-flow integration, and helping ensure that LLM-generated results are rendered and handled safely. |
| Utkarsh Shendre | 2026H1120136P | gRPC, backend, database, and final integration | Implemented and integrated the service boundaries, authentication and order workflows, SQLite persistence, inventory rules, forecast calculations, cache invalidation, validation, testing, and defect fixes. Key learning: service contracts and transactional data updates must keep inventory, sales history, analytics, and forecasts consistent. |
| Bhojani Karan Yogeshbhai | 2026H1120146P | Frontend, backend, and LLM integration *(proposed)* | Contributed across the user interface, backend service flow, and Ollama-backed LLM workflow. This includes supporting clear dashboard behaviour, reliable service communication, structured forecast and analytics responses, prompt design, validation, and error handling. |

## Individual reflections

### B Aditya Pai — Frontend, backend, and LLM integration *(proposed)*

Working across the frontend, backend, and LLM workflow demonstrates how every
layer of the system affects the user experience. The dashboard must present
current stock, predicted demand, reorder decisions, and AI insights clearly,
while the backend must provide reliable data and the LLM workflow must handle
slow or invalid responses safely. The main learning is that clear UI feedback,
well-defined service contracts, and validated AI output are equally important
to a dependable application.

### Utkarsh Shendre — gRPC, backend, database, and integration

Working on the backend and final integration reinforced the importance of
clear ownership of data and responsibilities. gRPC made the boundaries between
the browser-facing service, application server, and LLM service explicit.
SQLite transactions ensured that an order cannot reduce stock without also
being recorded correctly. A significant lesson from debugging was that a
successful stock update alone is not sufficient: the corresponding sales
history must be updated in the same transaction so that the seven-day average,
demand direction, and forecast remain accurate. I also learned to use
structured validation and focused tests to make cross-service failures easier
to diagnose.

### Bhojani Karan Yogeshbhai — Frontend, backend, and LLM integration *(proposed)*

Contributing across the frontend, backend, and LLM integration highlights the
importance of connecting all layers of a distributed application carefully.
The model is useful for estimates and concise qualitative observations, but
deterministic business rules must remain in the backend and be presented
clearly in the interface. Important learnings include using JSON schemas,
validating every returned item, setting realistic local-model deadlines,
keeping prompts unambiguous, and communicating failures usefully to users.

## Overall learning

The project showed how a distributed application benefits from separating
responsibilities: the frontend presents information, the application server
owns business rules, the database preserves authoritative state, and the LLM
provides bounded assistance. The most important shared lesson is that
integration quality matters as much as individual features. Changes to an
order must flow consistently through the database, forecast inputs, analytics,
and user interface; otherwise the system can display conflicting information.
