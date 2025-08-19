# Data Pipeline Maintenance Homework

## Pipeline Ownership Matrix

### Team Structure
**Team of 4 Data Engineers:**
- Engineer A (Senior)
- Engineer B (Mid-level)  
- Engineer C (Mid-level)
- Engineer D (Junior)

### Pipeline Ownership Assignment

| Pipeline | Primary Owner | Secondary Owner | Rationale |
|----------|---------------|-----------------|-----------|
| **Unit-level Profit (Experiments)** | Engineer B | Engineer C | Mid-level engineers handle experimental data; frequent changes expected |
| **Aggregate Profit (Investors)** | Engineer A | Engineer B | Senior engineer owns investor-facing critical pipeline |
| **Daily Growth (Experiments)** | Engineer C | Engineer D | Mid-level primary with junior secondary for learning |
| **Aggregate Growth (Investors)** | Engineer A | Engineer C | Senior engineer owns investor-facing critical pipeline |
| **Aggregate Engagement (Investors)** | Engineer B | Engineer A | Mid-level primary with senior backup for investor data |

## On-Call Schedule

### Rotation Structure
**2-week rotations** with 24/7 coverage:

| Week | Primary On-Call | Secondary On-Call |
|------|----------------|-------------------|
| Week 1-2 | Engineer A | Engineer B |
| Week 3-4 | Engineer B | Engineer C |
| Week 5-6 | Engineer C | Engineer D |
| Week 7-8 | Engineer D | Engineer A |

### Holiday Coverage
- **Major holidays** (Christmas, New Year, July 4th): Senior engineer (A) automatically covers
- **Personal time off**: 2-week advance notice required
- **Holiday rotation**: Junior engineer gets first choice of holiday time off
- **Emergency coverage**: All engineers maintain 4-hour response SLA during holidays

### Escalation Path
1. **Primary on-call** (30 min response)
2. **Secondary on-call** (45 min response)  
3. **Senior engineer** (1 hour response)
4. **Engineering manager** (2 hour response)

## Runbooks for Investor-Facing Pipelines

### 1. Aggregate Profit Pipeline

#### **Common Failure Scenarios:**
- **Revenue data source delays**: Billing system batch processing failures
- **Cost allocation errors**: Incorrect department cost mapping
- **Currency conversion failures**: Exchange rate API downtime
- **Data quality issues**: Negative profit values, missing business units
- **ETL timeout errors**: Large data volume processing delays

#### **Monitoring & Alerts:**
- Pipeline completion SLA: 6 AM daily
- Data freshness: <24 hours
- Profit margin variance: >5% from previous day triggers alert
- Row count validation: ±2% from expected volume

### 2. Aggregate Growth Pipeline

#### **Common Failure Scenarios:**
- **User tracking data gaps**: Mobile app analytics outages
- **Attribution model failures**: Marketing channel mapping errors
- **Cohort calculation errors**: Incorrect user signup date attribution
- **Seasonality adjustment failures**: Holiday traffic normalization errors
- **Geographic aggregation issues**: Country-level data inconsistencies

#### **Monitoring & Alerts:**
- Pipeline completion SLA: 5 AM daily
- Growth rate variance: >10% day-over-day triggers investigation
- User acquisition data completeness: >95% required
- Cohort data integrity: Zero negative growth rates allowed

### 3. Aggregate Engagement Pipeline

#### **Common Failure Scenarios:**
- **Event tracking data loss**: Frontend analytics collection failures
- **Session timeout logic errors**: Incorrect user activity calculations
- **Feature flag impact**: A/B test data contamination
- **Bot traffic filtering failures**: Artificial engagement inflation
- **Cross-platform aggregation errors**: Mobile vs web data inconsistencies

#### **Monitoring & Alerts:**
- Pipeline completion SLA: 7 AM daily  
- Engagement metric variance: >15% from 7-day average triggers alert
- Event volume validation: ±5% from expected daily volume
- User engagement distribution: No single user >1% of total engagement

## General Pipeline Health

### **Data Quality Checks (All Pipelines):**
- Schema validation
- Null value percentage monitoring
- Duplicate record detection
- Date range validation
- Business logic validation

### **Infrastructure Monitoring:**
- CPU/Memory utilization
- Database connection pool health
- Disk space monitoring
- Network latency checks
- Dependency service availability

### **Incident Response Protocol:**
1. **Detection**: Automated monitoring alerts
2. **Assessment**: On-call engineer determines severity
3. **Communication**: Stakeholder notification within 30 minutes
4. **Resolution**: Fix implementation with timeline updates
5. **Post-mortem**: Root cause analysis for investor-facing pipeline failures