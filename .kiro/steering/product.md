# AmoCRM Data Exporter v2.0

A comprehensive Python application for exporting data from AmoCRM (CRM system) with advanced features:

- **Parallel data export** of deals, contacts, companies, events, users, and pipelines
- **Modern web interface** built with FastAPI for data viewing and export management
- **Data enrichment** with user information and pipeline details
- **Scalable worker system** using RabbitMQ and FastStream for distributed processing
- **Multiple storage options** - MongoDB (primary) with JSON file fallback
- **Data flattening** system for complex AmoCRM custom fields
- **Performance monitoring** and benchmark testing capabilities
- **Export to multiple formats** - JSON, Excel, Google Sheets

The system is designed for high-volume CRM data processing with resilience, monitoring, and horizontal scaling capabilities.