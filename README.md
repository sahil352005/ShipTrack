# 🚢 ShipTrack: Smart Logistics & Supply Chain Intelligence Platform

**ShipTrack** is a production-ready, enterprise-grade logistics intelligence platform designed for high-throughput fleet management and supply chain optimization. It utilizes a modern data stack to transform raw shipment events into actionable operational insights.

![Premium Dashboard](https://img.shields.io/badge/UI-Premium_Glassmorphism-blueviolet)
![Real-time](https://img.shields.io/badge/Streaming-Apache_Kafka-red)
![Analytics](https://img.shields.io/badge/Processing-Apache_Spark-orange)
![Security](https://img.shields.io/badge/Security-RBAC_JWT-green)

---

## 🌟 Key Enterprise Features

### 1. 🌍 Geo-Map Fleet Visibility
- **Live Tracking**: Real-time visualization of shipment coordinates using Leaflet.js with a high-contrast Dark Matter theme.
- **Status Coding**: Markers are color-coded (🟩 Delivered, 🨨 In-Transit, 🟥 Delayed, 🟦 Created) for instant operational awareness.
- **Intelligent Popups**: One-click access to shipment IDs, region, and real-time risk assessment scores.

### 2. 📊 Advanced Analytics & Visualization (Chart.js)
- **Volume Tracking**: Interactive time-series charts showing hourly shipment volume trends.
- **Regional Performance**: Delivery trend analysis comparing "Estimated vs. Actual" hours across all operation zones.
- **Executive Summary**: A high-level KPI dashboard showing on-time rates, SLA violations, and critical risk alerts.

### 3. 🔐 Enterprise Security & RBAC
- **Role-Based Access Control**: Granular permissions for **Admin**, **Manager**, and **Analyst** roles.
- **Stateless Authentication**: Secure JWT-based authentication flow with encrypted password hashing (Bcrypt).
- **Security Audit**: View-only mode for Analysts and full provisioning rights for Admins.

### 4. 📄 Professional Reporting Module
- **Executive PDFs**: High-fidelity PDF reports for stakeholders with summarized KPIs and branding.
- **Operational CSVs**: Full-detail exports for deep-dive analysis in Excel or BI tools.
- **Automated SLA Tracking**: Dedicated reporting for Service Level Agreement violations.

---

## 🏗️ Technical Architecture & Stack

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Ingestion** | Apache Kafka | High-velocity event streaming & message queuing |
| **ETL/Analytics** | Apache Spark | Real-time data transformation & risk calculation |
| **Database** | PostgreSQL 16 | Relational storage for raw events & aggregated KPIs |
| **Backend** | FastAPI | High-performance Python API with async support |
| **Auth** | JWT / Passlib | Secure stateless auth & password maintenance |
| **Frontend** | Vanilla JS / CSS | Custom Glassmorphism UI with Chart.js & Leaflet |

### Data Flow Diagram
`Producer (Python)` → `Kafka Topic (shipments)` → `Spark ETL` → `PostgreSQL` → `FastAPI` → `User Dashboard`

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop (Windows/Linux/Mac)
- 4GB+ Allocated RAM for Docker

### Deployment Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/sahil352005/ShipTrack.git
   cd ShipTrack
   ```

2. Launch the full environment:
   ```bash
   docker-compose up --build -d
   ```

3. Access Points:
   - **Main Portal**: [http://localhost:8000/login](http://localhost:8000/login)
   - **System Metadata**: [http://localhost:8000/docs](http://localhost:8000/docs) (Swagger UI)
   - **DB Explorer**: [http://localhost:8080](http://localhost:8080) (Adminer)

### Default Access Credentials
| Role | Email | Password |
| :--- | :--- | :--- |
| **Full Admin** | `admin@logistics.com` | `password123` |
| **Operations Manager** | `manager@logistics.com` | `password123` |
| **Data Analyst** | `analyst@logistics.com` | `password123` |

---

## ⚙️ Configuration & Environment

The platform can be configured via environment variables in `docker-compose.yml`:

- `POSTGRES_DB`: Name of the logistics database (default: `logistics`)
- `KAFKA_BOOTSTRAP_SERVERS`: Internal Kafka address (default: `kafka:9092`)
- `SECRET_KEY`: JWT signing secret for enterprise security.
- `REPLICATION_FACTOR`: Kafka internal replication (set to 1 for standalone demo).

---

## 🛠️ Troubleshooting

- **No Map Data?**: Check if the `shipments` table is populated in Adminer. If empty, ensure the `producer` and `spark-etl` containers are healthy.
- **Login 500 Error?**: Ensure the `users` table was correctly created during the initial PostgreSQL startup.
- **Spark Build Fail?**: Ensure you have a stable internet connection for Spark to download the required Kafka/PostgreSQL JAR dependencies on the first run.

---

## 🗺️ Roadmap
- [ ] Integration with Google Maps Premium for traffic-aware routing.
- [ ] Predictive maintenance alerts using Spark MLLib.
- [ ] Mobile-native application for drivers.

---

## 📜 License
This project is licensed under the MIT License - see the `LICENSE` file for details.
