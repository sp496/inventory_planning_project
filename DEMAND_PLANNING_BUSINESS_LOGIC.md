# Demand Planning Business Logic Documentation

## Table of Contents
1. [Overview](#overview)
2. [Business Purpose](#business-purpose)
3. [Data Sources](#data-sources)
4. [Patient Eligibility Logic](#patient-eligibility-logic)
5. [Treatment Mapping Logic](#treatment-mapping-logic)
6. [Medicine Requirement Calculation](#medicine-requirement-calculation)
7. [Visit Projection Logic](#visit-projection-logic)
8. [Business Cases & Scenarios](#business-cases--scenarios)
9. [Constraints & Rules](#constraints--rules)
10. [Output Structure](#output-structure)

---

## Overview

The Demand Planning system forecasts future medication requirements for clinical trial subjects by projecting their upcoming visit schedules. It analyzes each active patient's treatment protocol, their last recorded visit, and projects future visits based on cycle patterns, ultimately calculating the quantity of medicine needed at each projected visit.

---

## Business Purpose

**Primary Goal**: Forecast medication demand for clinical inventory planning

**Key Objectives**:
- Project future patient visits based on treatment cycles
- Calculate medication quantities needed per visit
- Support inventory planning for sites and depots
- Enable proactive supply chain management
- Forecast demand up to 365 days into the future

**Business Value**:
- Prevents medication stockouts at clinical sites
- Reduces wastage from over-ordering
- Improves patient care by ensuring medication availability
- Optimizes supply chain costs

---

## Data Sources

### 1. Subject Summary Data
Contains patient-level information including:
- **Subject Identification**: subject number, site, depot, country
- **Treatment Information**: randomized treatment, TPC (Treatment Protocol Code), subject status
- **Visit History**: last visit date, last visit description (e.g., "Cycle 20 Day 1"), visit numbers
- **Dates**: randomization date, crossover dates, next scheduled visit windows
- **Status Indicators**: subject status, additional drug status

### 2. Treatment Mapping Data
Defines treatment protocols including:
- **Treatment Protocol**: study protocol, randomized treatment, TPC, subject status
- **Medication Details**: primary drug dispensed, additional drugs
- **Visit Schedule**: visit days pattern (e.g., "1,8,15" = visits on days 1, 8, and 15 of each cycle)
- **Dispensing Rules**: quantity per visit, cycle length (frequency in days)
- **Constraints**: maximum number of cycles allowed

---

## Patient Eligibility Logic

### Inclusion Criteria
Patients are **included** in demand forecasting if they are active in the trial.

### Exclusion Criteria
Patients are **excluded** if their status is any of the following:
- **"Screen Failed"** - Did not meet screening criteria
- **"Pre-Screened Failed"** - Failed pre-screening
- **"Treatment Discontinued"** - Stopped primary treatment
- **"Crossover Treatment Discontinued"** - Stopped crossover treatment

### Rationale
Only active patients receiving treatment will have future medication needs. Excluded patients represent discontinued or failed enrollments.

---

## Treatment Mapping Logic

### How Patients are Matched to Treatment Protocols

The system uses a **hierarchical matching strategy** that prioritizes country-specific protocols over generic protocols.

**Base Matching Keys** (all must match):
1. **Study Protocol** - The clinical study identifier (e.g., "GS-US-592-6173")
2. **Randomized Treatment** - The treatment arm assigned at randomization
3. **TPC (Treatment Protocol Code)** - Additional treatment classification
4. **Subject Status** - Current patient status

### Hierarchical Country Matching

The mapping data can contain:
- **Country-specific protocols**: Where the `country` field has a value (e.g., "united states", "canada")
- **Generic protocols**: Where the `country` field is null (applies to all countries)

**Matching Priority**:

1. **First Priority - Country-Specific Match**:
   - Includes `country` in the matching keys
   - Matches patient's country exactly with mapping's country
   - Example: Patient in USA matches USA-specific protocol

2. **Fallback - Generic Match**:
   - Only for patients not matched in step 1
   - Matches only on base keys (excluding country)
   - Uses only mapping rows where country is null
   - Example: Patient in Canada with no Canada-specific protocol uses generic protocol

**Why This Approach?**
- Allows different drug dispensing rules per country
- Provides flexibility for country-specific regulations
- Maintains a default protocol for countries without specific rules
- Ensures every active patient gets matched

### Text Normalization
Before matching, all text fields are normalized:
- Convert to lowercase
- Strip leading/trailing whitespace
- Replace smart quotes with regular quotes
- Example: "Treatment A" and "treatment a" match

### Medicine Identification

For each patient-treatment match, the system identifies which medicine(s) they receive:

**Primary Medicine**:
- If `study_drug_dispensed` is specified → use this medicine

**Additional Medicine**:
- If `additional_study_drug_dispensed` is specified → use this medicine
- A patient may receive both primary and additional medicines (creates separate forecast rows)

**Result**: Each patient-medicine combination becomes a unique record for visit projection.

---

## Medicine Requirement Calculation

### Visit Count Per Cycle

**Formula**:
```
Visit Count = Number of days in visit_days pattern
```

**Example**:
- `visit_days = "1,8,15"` → 3 visits per cycle
- `visit_days = "1"` → 1 visit per cycle

### Total Medicine Required Per Cycle

**Formula**:
```
Total Medicine Per Cycle = Visit Count × Dispensing Quantity
```

**Example 1**:
- Visit days: "1,8,15" → 3 visits
- Dispensing quantity: 2 bottles per visit
- **Result**: 3 × 2 = **6 bottles per cycle**

**Example 2**:
- Visit days: "1" → 1 visit
- Dispensing quantity: 5 bottles per visit
- **Result**: 1 × 5 = **5 bottles per cycle**

### Aggregation by Patient-Medicine
If a patient has multiple treatment mappings resulting in the same medicine (edge case), the medicine requirements are summed together.

---

## Visit Projection Logic

This is the **core calculation** that projects future visit dates and medicine needs.

### Input Information for Projection

For each patient-medicine combination:
- **Last Visit Date**: The date of their most recent visit
- **Last Visit Description**: Text describing the visit (e.g., "TPC Cycle 20 Day 8", "Crossover Cycle 5 Day 1")
- **Visit Pattern**: Days within a cycle when visits occur (e.g., "1,8,15")
- **Cycle Length**: Duration of one cycle in days (e.g., 28 days)
- **Max Cycles**: Optional hard cap on number of cycles
- **Total Medicine Per Cycle**: Calculated medicine requirement

### Parsing Last Visit Information

**Cycle Number Parsing**:
The system extracts the cycle number from visit descriptions:
- "TPC C20D1" → Cycle 20
- "Cycle 46 Day 8" → Cycle 46
- "Crossover Cycle 5 Day 1" → Cycle 5
- Uses regex pattern: `(?:C|Cycle\s?)(\d+)`

**Day Number Parsing**:
The system extracts the day within the cycle:
- "TPC C20D1" → Day 1
- "Cycle 46 Day 8" → Day 8
- Uses regex pattern: `(?:D|Day\s?)(\d+)`

**Prefix Identification**:
- If "Crossover" appears → Prefix = "Crossover "
- If "TPC" appears → Prefix = "TPC "
- Otherwise → Prefix = "" (standard treatment)

### Projection Time Horizon

**Primary Constraint** (Time-Based):
- **365 days from today** - This is the main limiting factor
- Only visits within the next year are projected

**Secondary Constraint** (Cycle-Based):
- **max_cycles** - Optional hard cap on cycle numbers
- If defined, no visits beyond this cycle number are projected
- If not defined, only time constraint applies

### Visit Projection Algorithm

The system projects visits in **two phases**:

---

#### **Phase A: Remaining Visits in Current Cycle**

**Purpose**: Complete the cycle the patient is currently in

**Logic**:
1. Calculate Day 1 of the current cycle:
   ```
   Current Cycle Day 1 = Last Visit Date - (Last Day Number - 1)
   ```

   **Example**:
   - Last visit: 2025-01-15 (Cycle 20 Day 8)
   - Current Cycle Day 1 = 2025-01-15 - 7 days = 2025-01-08

2. Identify remaining visit days:
   ```
   Remaining Days = All visit days > Last Day Number
   ```

   **Example**:
   - Visit pattern: "1,8,15"
   - Last day: 8
   - Remaining: [15]

3. Project each remaining day:
   ```
   Visit Date = Current Cycle Day 1 + (Day - 1)
   ```

   **Example**:
   - Day 15: 2025-01-08 + 14 = 2025-01-22

**Filters Applied**:
- Visit date must be **after** last recorded visit
- Visit date must be **within 365 days** from today
- Cycle number must be **≤ max_cycles** (if defined)

---

#### **Phase B: Future Complete Cycles**

**Purpose**: Project all upcoming cycles within the time horizon

**Step 1: Calculate Next Cycle Start**
```
Next Cycle Day 1 = Current Cycle Day 1 + Cycle Duration
```

**Example**:
- Current Cycle Day 1: 2025-01-08
- Cycle duration: 28 days
- Next Cycle Day 1: 2025-02-05

**Step 2: Roll Forward if in the Past**

If the calculated start date has already passed (patient missed cycles):
```
Missed Cycles = (TODAY - Next Cycle Day 1) ÷ Cycle Duration + 1
Adjusted Start = Next Cycle Day 1 + (Missed Cycles × Cycle Duration)
```

**Example**:
- Next Cycle Day 1: 2024-12-01 (in the past)
- Today: 2025-01-10
- Cycle duration: 28 days
- Days since start: 40 days
- Missed cycles: 40 ÷ 28 + 1 = 2 cycles
- Adjusted Start: 2024-12-01 + 56 = 2025-01-26

**Step 3: Project All Future Cycles**

Loop through cycles until time horizon exceeded:

```
For each cycle offset (0, 1, 2, 3, ...):
    Projected Cycle Number = Next Cycle Number + Offset
    Cycle Day 1 = Adjusted Start + (Offset × Cycle Duration)

    For each visit day in pattern:
        Visit Date = Cycle Day 1 + (Day - 1)

        If Visit Date ≤ Time Horizon AND Projected Cycle Number ≤ Max Cycles:
            Create projected visit record
```

**Loop Termination**:
- Stops when **no visits** in a cycle fall within the time horizon
- Or when **max_cycles** is exceeded

---

### Visit Projection Example Walkthrough

**Patient Scenario**:
- Last visit: 2025-01-15 (Cycle 20 Day 8)
- Visit pattern: "1,8,15"
- Cycle duration: 28 days
- Max cycles: 25
- Today: 2025-01-10
- Time horizon: 2025-01-09 (365 days ahead)

**Phase A - Current Cycle (Cycle 20)**:
- Current Cycle Day 1: 2025-01-08
- Remaining days: [15]
- Projected visit:
  - Day 15: 2025-01-22 ✓ (within horizon, after last visit)

**Phase B - Future Cycles**:

**Cycle 21**:
- Day 1: 2025-02-05
- Projected visits:
  - Day 1: 2025-02-05 ✓
  - Day 8: 2025-02-12 ✓
  - Day 15: 2025-02-19 ✓

**Cycle 22**:
- Day 1: 2025-03-05
- Projected visits:
  - Day 1: 2025-03-05 ✓
  - Day 8: 2025-03-12 ✓
  - Day 15: 2025-03-19 ✓

... continues until ...

**Cycle 25** (last cycle due to max_cycles = 25):
- Day 1: 2025-05-30
- Projected visits:
  - Day 1: 2025-05-30 ✓
  - Day 8: 2025-06-06 ✓
  - Day 15: 2025-06-13 ✓

**Cycle 26**:
- Skipped because 26 > max_cycles

---

## Business Cases & Scenarios

### Case 1: Standard Treatment Patient

**Characteristics**:
- Regular treatment cycle
- No crossover
- Has defined visit pattern

**Example**:
- Last visit: "Cycle 10 Day 1"
- Visit pattern: "1,15"
- Cycle duration: 28 days

**Projection**:
- Remaining visits in Cycle 10: Day 15
- Future cycles: 11, 12, 13, ... until time/cycle limit

**Output Visit Descriptions**:
- "Cycle 10 Day 15"
- "Cycle 11 Day 1"
- "Cycle 11 Day 15"
- etc.

---

### Case 2: TPC (Treatment Protocol Code) Patient

**Characteristics**:
- Patient is on a TPC-designated treatment
- Visit descriptions include "TPC" prefix

**Example**:
- Last visit: "TPC C20D8"
- Visit pattern: "1,8,15"
- Cycle duration: 28 days

**Projection**:
- System identifies TPC from visit description
- All future visits include "TPC " prefix

**Output Visit Descriptions**:
- "TPC Cycle 20 Day 15"
- "TPC Cycle 21 Day 1"
- "TPC Cycle 21 Day 8"
- etc.

---

### Case 3: Crossover Patient

**Characteristics**:
- Patient switched from original treatment to crossover treatment
- Different medication may be dispensed
- Visit descriptions include "Crossover"

**Example**:
- Last visit: "Crossover Cycle 5 Day 1"
- Visit pattern: "1,8"
- Cycle duration: 21 days
- Medicine: Different from original randomized treatment

**Projection**:
- System identifies Crossover from visit description
- All future visits include "Crossover " prefix

**Output Visit Descriptions**:
- "Crossover Cycle 5 Day 8"
- "Crossover Cycle 6 Day 1"
- "Crossover Cycle 6 Day 8"
- etc.

---

### Case 4: Patient Behind Schedule (Missed Visits)

**Characteristics**:
- Next calculated cycle start is in the past
- Patient has missed scheduled cycles

**Example**:
- Last visit: 2024-11-01 (Cycle 10 Day 1)
- Cycle duration: 28 days
- Today: 2025-01-10
- Next cycle should have started: 2024-11-29 (in the past)

**Roll-Forward Calculation**:
- Days since next cycle start: 42 days
- Missed cycles: 42 ÷ 28 + 1 = 2 cycles
- Adjusted next start: 2024-11-29 + 56 = 2025-01-24

**Projection**:
- Starts projecting from Cycle 13 (10 + 2 missed + 1 next)
- First projected visit: 2025-01-24

**Business Rationale**:
- Assumes patient will resume treatment
- Projects forward to realistic future dates
- Prevents projecting visits in the past

---

### Case 5: Patient with Max Cycles Limit

**Characteristics**:
- Treatment protocol has a defined maximum number of cycles
- Common in fixed-duration trials

**Example**:
- Current cycle: 48
- Max cycles: 50
- Cycle duration: 28 days
- Visit pattern: "1,15"

**Projection**:
- Cycle 48: Remaining visits
- Cycle 49: All visits (1, 15)
- Cycle 50: All visits (1, 15)
- Cycle 51: **STOP** - exceeds max_cycles

**Business Rationale**:
- Prevents over-ordering for patients nearing treatment completion
- Aligns with protocol-defined treatment duration

---

### Case 6: Patient with Single Visit Per Cycle

**Characteristics**:
- Visit pattern has only one day
- Common in maintenance phases

**Example**:
- Visit pattern: "1"
- Cycle duration: 28 days

**Projection**:
- Only Day 1 of each cycle is projected
- Simpler forecast pattern

**Medicine Calculation**:
- Visits per cycle: 1
- Medicine per cycle: 1 × dispensing quantity

---

### Case 7: Patient with Multiple Medicines

**Characteristics**:
- Patient receives both primary and additional study drugs
- Each medicine has separate forecasting

**Example**:
- Primary drug: "Drug A" (dispensing qty: 2)
- Additional drug: "Drug B" (dispensing qty: 1)
- Visit pattern: "1,15"

**Result**:
- Two separate forecast streams:
  - Drug A: 2 bottles × 2 visits = 4 bottles per cycle
  - Drug B: 1 bottle × 2 visits = 2 bottles per cycle

**Output**:
- Separate rows for each drug in final forecast
- Same visit dates, different medicine and quantities

---

### Case 8: Country-Specific vs Generic Protocol Matching

**Characteristics**:
- Some patients match country-specific protocols
- Other patients fall back to generic protocols

**Mapping Data**:
```
Row 1: study=GS-123, treatment=A, tpc=TPC1, status=OnTreatment, country=USA
       drug=DrugX-USA, dispensing_qty=3

Row 2: study=GS-123, treatment=A, tpc=TPC1, status=OnTreatment, country=null
       drug=DrugX-Generic, dispensing_qty=2
```

**Scenario A - Country-Specific Match**:
- Patient in USA
- **Match**: Row 1 (country-specific)
- Drug dispensed: DrugX-USA
- Quantity per visit: 3

**Scenario B - Generic Fallback**:
- Patient in Canada (no Canada-specific mapping exists)
- **Match**: Row 2 (generic, country=null)
- Drug dispensed: DrugX-Generic
- Quantity per visit: 2

**Business Rationale**:
- Allows country-specific regulations (e.g., different package sizes)
- USA patients get USA-specific protocol
- Other countries use default protocol
- Ensures all patients are covered

**Important**: Patients never match BOTH rows - country-specific match takes priority.

---

### Case 9: Patient with Complex Visit Pattern

**Characteristics**:
- Multiple visits per cycle at irregular intervals

**Example**:
- Visit pattern: "1,4,8,11,15,18,22,25"
- Cycle duration: 28 days
- Last visit: Day 15

**Projection for Current Cycle**:
- Remaining days: 18, 22, 25
- Three more visits in current cycle

**Projection for Future Cycles**:
- Each cycle: 8 visits
- High medicine requirement per cycle

**Business Implication**:
- Intensive treatment schedule
- Higher inventory needs

---

## Constraints & Rules

### Time Constraints

**365-Day Projection Window**:
- Primary limiting factor
- Calculated as: Today + 365 days
- No visits projected beyond this date
- **Rationale**: Balance between planning horizon and forecast accuracy

### Cycle Constraints

**Max Cycles (Optional)**:
- Hard cap on cycle numbers
- Protocol-specific
- If undefined: Only time constraint applies
- If defined: Acts as secondary constraint

**Cycle Duration**:
- Defined in `dispensing_frequency_days`
- Typically 21, 28, or 42 days
- Determines spacing between cycle starts

### Data Quality Rules

**Visit Pattern Parsing**:
- Visit days must be comma-separated integers
- Invalid or missing patterns default to "1"
- Example valid: "1,8,15"
- Example invalid: "Day 1, Day 8" (will fail to parse)

**Date Validation**:
- Last visit date must be valid timestamp
- Patients with invalid dates are skipped
- No visits projected if last visit date is null

**Cycle Duration Validation**:
- Must be a positive number
- Defaults to 28 if invalid
- Cannot be zero or negative

### Visit Inclusion Rules

A projected visit is included in the forecast **only if ALL**of the following are true:

1. **After Last Recorded Visit**: `Visit Date > Last Visit Date`
   - Prevents duplicate counting

2. **Within Time Horizon**: `Visit Date ≤ (Today + 365 days)`
   - Primary time constraint

3. **Within Cycle Limit**: `Cycle Number ≤ Max Cycles` (if max_cycles defined)
   - Protocol compliance constraint

4. **Valid Medicine**: `Drug Dispensed ≠ "nan"` and not null
   - Data quality requirement

### Aggregation Rules

**Patient-Medicine Uniqueness**:
- Each patient-medicine combination is unique
- If multiple mappings result in same medicine, requirements are summed

**Country Handling**:
- After merge, subject's country and mapping country may differ
- Subject's country (country_x) is preserved as "subject_country"
- Mapping country (country_y) is used for protocol matching

---

## Output Structure

### Final Output Columns

| Column | Description | Example |
|--------|-------------|---------|
| `study_name` | Study protocol identifier | "gs-us-592-6173" |
| `parent_depot` | Parent depot ID | 5001 |
| `site_id` | Clinical site ID | 101 |
| `subject_number` | Patient identifier | 90456 |
| `subject_status` | Patient status | "On Treatment" |
| `subject_country` | Patient's country | "united states" |
| `randomized_treatment` | Treatment arm | "treatment a" |
| `tpc` | Treatment protocol code | "tpc1" |
| `drug_dispensed` | Medicine name | "drug xyz" |
| `dispensing_quantity` | Bottles per visit | 2 |
| `predicted_study_visit` | Visit description | "Cycle 21 Day 8" |
| `cycle` | Cycle number | 21 |
| `day` | Day within cycle | 8 |
| `predicted_next_visit_date` | Projected visit date | "2025-03-15" |
| `processed_timestamp` | Forecast run date | "2025-01-10" |

### Output Characteristics

**Granularity**: One row per projected visit
- Each visit represents one medicine dispensing event
- Multiple rows per patient (one per visit per medicine)

**Sorting**:
- Study → Depot → Site → Subject → Cycle → Day
- Enables easy review by site and depot

**Volume**:
- Typical patient: 20-50 projected visits (over 365 days)
- Multi-visit cycles generate more rows
- Patients with multiple medicines generate separate rows

### Output Use Cases

**Depot Planning**:
- Aggregate by depot + drug + month
- Forecast total medicine needs per depot

**Site Planning**:
- Aggregate by site + drug + month
- Ensure sufficient site-level inventory

**Study-Level Planning**:
- Aggregate by study + drug + month
- Overall trial medicine requirements

**Patient-Level Tracking**:
- Filter by subject_number
- Review individual patient schedules

---

## Summary of Key Business Rules

1. **Only active patients** are included in forecasting

2. **Hierarchical country matching**: Country-specific protocols take priority over generic (null country) protocols

3. **365 days** is the primary projection horizon

4. **Max cycles** (if defined) provides a secondary hard cap

5. **Time-based projection** takes precedence over cycle-based

6. **Missed cycles are rolled forward** to realistic future dates

7. **Each patient-medicine combination** generates independent forecasts

8. **Visit descriptions preserve prefix** (Crossover, TPC) for traceability

9. **Medicine quantity** = Visits per cycle × Dispensing quantity per visit

10. **Remaining current cycle visits** are projected first, then future cycles

11. **Projection stops** when visits exceed both time and cycle limits

---

## Validation & Quality Checks

### Input Validation
- Exclude patients with invalid statuses
- Skip records with missing last visit dates
- Handle unparseable visit patterns gracefully
- Default missing cycle durations to 28 days

### Output Validation
- All projected dates are in the future
- No duplicate visit records
- Cycle and day numbers are positive integers
- Medicine names are valid (not "nan")

### Business Logic Validation
- Verify projected dates follow cycle duration
- Confirm visits align with visit pattern
- Check that cycles increment properly
- Ensure max_cycles constraint is enforced

---

## Document Version
- **Created**: Based on demand_planning.py and demand_planning_dbricks.py from main branch
- **Business Logic Version**: Reflects 365-day time-based projection with optional max_cycles cap
- **Last Updated**: 2025-01-12
