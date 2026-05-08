from survey_kit_data.usda.snap import (
    snap_persons_snapshot,
    snap_households_snapshot,
    snap_benefits_snapshot,
    snap_monthly,
    snap_state_history,
    snap_county_history,
)

# State-level 3-month snapshots (most recent data only)
persons = snap_persons_snapshot().collect()
print("SNAP Persons (state snapshot)")
print(persons)

households = snap_households_snapshot().collect()
print("\nSNAP Households (state snapshot)")
print(households)

benefits = snap_benefits_snapshot().collect()
print("\nSNAP Benefits (state snapshot)")
print(benefits)

# National fiscal-year / monthly aggregate history
monthly = snap_monthly().collect()
print("\nSNAP Monthly national history")
print(monthly)

# State-level monthly history (FY1989–present)
state_history = snap_state_history().collect()
print("\nSNAP State history")
print(state_history)

# County-level snapshots (Jan and Jul, 1989–present)
county_history = snap_county_history().collect()
print("\nSNAP County history")
print(county_history)
