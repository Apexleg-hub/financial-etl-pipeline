┌─────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE ARCHITECTURE                │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌───────────┐  ┌───────────┐  ┌─────────────────────┐    │
│  │   EXTRACT │  │ TRANSFORM │  │       LOAD          │    │
│  │  (Layer)  │  │  (Layer)  │  │     (Layer)         │    │
│  └─────┬─────┘  └─────┬─────┘  └──────────┬──────────┘    │
│        │               │                   │               │
│  ┌─────▼─────┐  ┌─────▼─────┐  ┌──────────▼──────────┐    │
│  │ API Clients│  │Data Cleaner│  │   Supabase        │    │
│  │• Alpha V  │  │• Validation│  │   Loader           │    │
│  │• Finnhub  │  │• Standard. │  │   (Upserts)        │    │
│  │• FRED     │  │• Features  │  └────────────────────┘    │
│  │• Crypto   │  └─────────────┘                           │
│  │• Weather  │                                            │
│  │• Sentiment│  ┌────────────────────┐                    │
│  └───────────┘  │  Orchestration     │                    │
│                 │  • Apache Airflow  │                    │
│  ┌───────────┐  │  • Task DAGs       │                    │
│  │ Config    │  │  • Scheduling      │                    │
│  │ Manager   │  │  • Monitoring      │                    │
│  └───────────┘  └────────────────────┘                    │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │               Monitoring & Logging                  │   │
│  │  • Structured Logging (JSON)                       │   │
│  │  • Metrics Collection                              │   │
│  │  • Alerting (Failed Jobs, Anomalies)              │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘