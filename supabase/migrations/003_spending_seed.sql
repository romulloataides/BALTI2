-- 003_spending_seed.sql
-- Conditionally seeds spending_events with sample data if the table is empty.
-- Safe to re-run: the DO block checks for existing rows before inserting.

do $$
begin
  if not exists (select 1 from public.spending_events limit 1) then

    insert into public.spending_events
      (nsa, category, program_name, amount, started_on, completed_on, status, source, details)
    values
      -- Road/infrastructure
      ('Southwest Baltimore',      'Road Resurfacing',             'Wilkens Ave Corridor Resurfacing',                425000.00, '2023-04-01', '2023-09-15', 'completed',    'DOT',  'Full-depth reclamation on Wilkens Ave from Carey St to Caton Ave. Includes curb repair and ADA ramp upgrades.'),
      ('Sandtown-Winchester/Harlem Park', 'Road Resurfacing',      'N Gilmor St and W North Ave Paving',              312000.00, '2022-06-01', '2022-11-30', 'completed',    'DOT',  'Mill-and-overlay resurfacing. W North Ave signal timing also updated.'),
      ('Cherry Hill',              'Road Resurfacing',             'Cherry Hill Rd / Seagull Ave Resurfacing',        280000.00, '2024-03-01', NULL,          'active',       'DOT',  'Phase 1 of 2-year Cherry Hill road improvement plan. Includes pothole patching and drainage work.'),
      ('Belair-Edison',            'Sidewalk Repair',              'Belair Rd Sidewalk Reconstruction',               198000.00, '2023-08-15', '2024-01-20', 'completed',    'DOT',  'ADA-compliant sidewalk reconstruction on Belair Rd between Erdman Ave and Argonne Dr.'),
      ('Canton',                   'Sidewalk Repair',              'Boston St Waterfront Sidewalk Repair',             87500.00, '2024-02-01', NULL,          'active',       'DOT',  'Concrete panel replacement and expansion joint sealing. High-pedestrian corridor.'),

      -- Vacant property / housing
      ('Southwest Baltimore',      'Vacant Structure Demolition',  'Pratt St Block Clearance — Phase 2',              540000.00, '2023-07-01', '2024-02-28', 'completed',    'DHCD', 'Demolition of 9 vacant row homes on 2100 block of Pratt St. Site cleared for green space interim use.'),
      ('Sandtown-Winchester/Harlem Park', 'Vacant Structure Demolition', 'N Mount St Stabilization and Demo',         375000.00, '2022-10-01', '2023-04-30', 'completed',    'DHCD', 'Emergency stabilization of 4 structurally compromised vacants followed by full demolition.'),
      ('Clifton-Berea',            'Vacant Structure Demolition',  'Clifton Ave Vacant Row Home Clearance',           220000.00, '2024-01-15', NULL,          'active',       'DHCD', 'Demolition of 6 long-term vacants identified in 2023 housing survey.'),
      ('Edmondson Village',        'Housing Rehabilitation',       'Edmondson Ave Homeowner Repair Program',          650000.00, '2023-01-01', '2023-12-31', 'completed',    'DHCD', 'Owner-occupied repair grants for 22 households. Roof, electrical, and lead abatement focus.'),

      -- Environmental / lead
      ('Southwest Baltimore',      'Lead Paint Remediation',       'Carrollton Ridge Lead Safe Homes Initiative',     890000.00, '2022-09-01', '2024-06-30', 'active',       'DHCD', 'HUD-funded lead hazard reduction in 45 pre-1978 rental units. Priority: households with children under 6.'),
      ('Clifton-Berea',            'Lead Paint Remediation',       'Clifton-Berea Lead Hazard Control Grant',         720000.00, '2023-03-01', NULL,          'active',       'DHCD', 'EPA grant-funded. Targets highest-risk properties identified through blood lead screening data.'),
      ('Cherry Hill',              'Lead Paint Remediation',       'Cherry Hill Public Housing Lead Abatement',       1200000.00,'2021-07-01', '2022-12-31', 'completed',    'HABC', 'HABC-managed abatement in 78 public housing units. Clearance testing completed with all units passing.'),

      -- Parks and green space
      ('Canton',                   'Park Renovation',              'Canton Cove Waterfront Park Improvements',        340000.00, '2023-05-01', '2023-10-15', 'completed',    'BCRP', 'New seating, ADA path upgrades, and stormwater bioretention planting along waterfront.'),
      ('Cherry Hill',              'Park Renovation',              'Cherry Hill Community Center Athletic Fields',    480000.00, '2022-04-01', '2022-10-31', 'completed',    'BCRP', 'Synthetic turf replacement on 2 fields. New LED lighting and bleacher repair.'),
      ('Southwest Baltimore',      'Tree Canopy Expansion',        'Wilkens Ave Urban Forestry Planting',              62000.00, '2024-04-01', NULL,          'active',       'DPW',  '120 new street trees planted along Wilkens Ave corridor. 3-year maintenance agreement included.'),
      ('Greater Charles Village/Barclay', 'Park Renovation',      'Barclay-Greenmount Linear Park Phase 1',          290000.00, '2023-09-01', NULL,          'active',       'BCRP', 'New walking path, lighting, and community garden plots. Developed with neighborhood association.'),

      -- Sanitation / 311 response
      ('Southwest Baltimore',      'Illegal Dumping Cleanup',      'Carrollton Ridge Alley Sanitation Surge',          48000.00, '2023-06-01', '2023-08-31', 'completed',    'DPW',  '14-week intensive alley cleaning and illegal dump site abatement. Cameras installed at 3 chronic sites.'),
      ('Sandtown-Winchester/Harlem Park', 'Illegal Dumping Cleanup','Sandtown Alley Watch Program',                   75000.00, '2022-05-01', '2022-11-30', 'completed',    'DPW',  'Combination of alley cleaning, waste receptacle installation, and community steward stipends.'),
      ('Cherry Hill',              'Sanitation Equipment',         'Cherry Hill Transfer Station Upgrade',            2400000.00,'2021-10-01', '2023-03-31', 'completed',    'DPW',  'Full renovation of Quarantine Rd transfer station. Increased capacity by 40%. Odor control systems installed.'),

      -- Lighting
      ('Southwest Baltimore',      'Streetlight Replacement',      'Wilkens / Caton Corridor LED Conversion',         155000.00, '2023-03-01', '2023-07-31', 'completed',    'BGE/DPW', 'Replacement of 210 HPS fixtures with LED streetlights. Average illumination increased 35%.'),
      ('Sandtown-Winchester/Harlem Park', 'Streetlight Replacement','N Monroe / W Baltimore LED Upgrade',             132000.00, '2022-08-01', '2022-12-15', 'completed',    'BGE/DPW', '180 LED conversions. Resident survey showed 61% reported feeling safer after installation.'),

      -- Schools
      ('Belair-Edison',            'School Facility Maintenance',  'Belair Edison Middle School HVAC Replacement',    1850000.00,'2022-07-01', '2023-01-31', 'completed',    'BCPS', 'Full HVAC system replacement including air quality monitoring sensors in all classrooms.'),
      ('Cherry Hill',              'School Facility Maintenance',  'Cherry Hill Elementary Roof and Window Repair',   620000.00, '2023-07-01', '2023-12-20', 'completed',    'BCPS', 'Roof membrane replacement and 64 window units. Addresses long-standing water infiltration.'),

      -- Economic / broadband
      ('Southwest Baltimore',      'Broadband Infrastructure',     'Carrollton Ridge Digital Equity Pilot',           185000.00, '2023-10-01', NULL,          'active',       'MOBD', 'Free in-home broadband connections for 320 low-income households. Includes device lending and digital literacy training.'),
      ('Sandtown-Winchester/Harlem Park', 'Community Programs',    'Sandtown Workforce Re-Entry Job Training',        240000.00, '2024-01-01', NULL,          'active',       'MOED', 'Partnership with Humanim and Johns Hopkins for 18-month CNC and construction trades training cohort.');

  end if;
end $$;
