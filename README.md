#!/usr/bin/env python3
"""
generate_healthcare_500k_full.py

- Produces ONE flat CSV with 500,000 visit-level rows.
- Each patient repeats 1..4 times (max 4 visits).
- Doctors & Staff may overlap (some IDs are the same person).
- AdmissionDate & DischargeDate are always populated.
- 35 ID columns (each with descriptive columns) included.
- Writes in chunks to avoid high memory use.
"""

import csv
import random
import math
from datetime import datetime, timedelta
from faker import Faker

# ------------- CONFIG -------------
SEED = 42
TOTAL_ROWS = 500_000             # 5 lakh rows
# To achieve max 4 visits per patient and keep revisits realistic,
# pick number of unique patients so average visits ~2.5
AVG_VISITS = 2.5
UNIQUE_PATIENTS = max(1000, int(TOTAL_ROWS / AVG_VISITS))  # ~200k for 500k rows
UNIQUE_DOCTORS = 2000
UNIQUE_STAFF = 2500
UNIQUE_BEDS = 3000
UNIQUE_EQUIPMENT = 1200
UNIQUE_MEDICINES = 2500
UNIQUE_HOSPITALS = 300

CHUNK_SIZE = 50_000  # flush rows to disk per chunk

# Probabilities & rules
P_DOCTOR_ALSO_STAFF = 0.10       # 10% of doctors also are staff (same person)
P_STAFF_ACTING_AS_DOCTOR = 0.06  # 6% of staff will act as doctor on a visit
P_ADMISSION = 0.35
P_DISCHARGE_SAME_DAY = 0.6
P_MULTI_DIAGNOSIS = 0.2

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 8, 1)

random.seed(SEED)
faker = Faker()
Faker.seed(SEED)

# ------------- MASTER DIMENSIONS (ID, NAME pairs) -------------
def make_pairs(names):
    return [(i+1, n) for i, n in enumerate(names)]

Gender = make_pairs(["Male", "Female", "Other"])
States = make_pairs([
    "Maharashtra","Karnataka","Gujarat","Delhi","Tamil Nadu","Telangana","West Bengal","Rajasthan",
    "Madhya Pradesh","Uttar Pradesh","Punjab","Haryana","Kerala","Bihar","Odisha"
])
Cities = make_pairs([
    "Mumbai","Pune","Nagpur","Bengaluru","Mysuru","Ahmedabad","Surat","Delhi","Chennai","Hyderabad",
    "Kolkata","Jaipur","Indore","Lucknow","Chandigarh","Kochi","Patna","Bhubaneswar"
])
Education = make_pairs(["MBBS","MD","MS","DNB","BAMS","BHMS","MCh","DM","PhD"])
Specialization = make_pairs([
    "General Medicine","Cardiology","Orthopedics","Neurology","Pediatrics","Dermatology",
    "ENT","Gynecology","Oncology","Gastroenterology","Psychiatry","Pulmonology","Nephrology"
])
EmploymentType = make_pairs(["Permanent","Visiting","Contract"])
Shift = make_pairs(["Morning","Evening","Night"])
StaffRole = make_pairs(["Nurse","Receptionist","Technician","Pharmacist","Cleaner","Security","Admin","Doctor"])
Department = make_pairs([
    "Emergency","ICU","General Ward","OPD","Radiology","Pathology","Surgery","Cardiology",
    "Orthopedics","Pediatrics","Gynecology","Oncology","Neurology"
])
BedType = make_pairs(["ICU","General","Private","Semi-Private","Pediatric"])
BedStatus = make_pairs(["Occupied","Available","Maintenance","Reserved"])
EquipmentType = make_pairs(["MRI","CT Scan","X-Ray","Ventilator","ECG","Ultrasound","Infusion Pump","Defibrillator","Surgical Light"])
EquipmentCondition = make_pairs(["Good","Needs Repair","Out of Service","Replaced"])
MedicineUsage = make_pairs(["Fever","Headache","Stomach Pain","Cold & Cough","Infection","Hypertension","Diabetes","Body Pain"])
DosageStrength = make_pairs(["50 mg","100 mg","200 mg","250 mg","500 mg"])
MedicineType = make_pairs(["Tablet","Capsule","Injection","Insulin","Tonic","Syrup","Drops"])
MedicineBrand = make_pairs(["Cipla","Sun Pharma","Dr. Reddy's","Pfizer","GSK","Abbott","Lupin","Aurobindo"])
MedicineCategory = make_pairs(["Painkiller","Antibiotic","Antifungal","Antiviral","Antipyretic","Antihypertensive","Antidiabetic"])
MedicineColour = make_pairs(["White","Red","Blue","Green","Yellow","Pink","Orange"])
InsuranceProvider = make_pairs(["Star Health","ICICI Lombard","HDFC Ergo","New India","Bajaj Allianz","United India","SBI General"])
PaymentMethod = make_pairs(["Cash","Card","UPI","Insurance"])
PaymentStatus = make_pairs(["Paid","Pending","Partially Paid","Rejected"])
VisitReason = make_pairs(["Check-up","Follow-up","Emergency","Surgery","Diagnosis","Vaccination","Therapy Session"])
Diagnosis = make_pairs([
    "Viral Fever","Migraine","Gastritis","Fracture","Asthma","Hypertension","Diabetes","Dermatitis",
    "Otitis Media","Anemia","COVID-19","Pneumonia","UTI"
])
TestType = make_pairs(["Blood Test","X-Ray","MRI","CT Scan","ECG","Ultrasound","Urine Test","Spirometry"])
HospitalSpecializationList = ["Multi-Specialty", "Cardiac", "Pediatric", "Cancer Care", "Neurology Center",
                              "Orthopedic", "Maternity", "Eye Care", "ENT", "General"]
GovPrivate = ["Government", "Private"]

# ------------- Utility helpers -------------
def make_code(prefix, n, width=6):
    return [f"{prefix}{str(i+1).zfill(width)}" for i in range(n)]

def pick(pairs):
    return random.choice(pairs)

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days),
                             seconds=random.randint(0, 86399))

def days_between(a, b):
    return max(0, (b - a).days)

# ------------- Pools & catalogs -------------
PatientIDs = make_code("PAT", UNIQUE_PATIENTS, width=7)
DoctorIDs = make_code("DOC", UNIQUE_DOCTORS)
StaffIDs = make_code("STF", UNIQUE_STAFF)
BedIDs = make_code("BED", UNIQUE_BEDS)
EquipmentIDs = make_code("EQP", UNIQUE_EQUIPMENT)
MedicineIDs = make_code("MED", UNIQUE_MEDICINES)
HospitalIDs = make_code("HOS", UNIQUE_HOSPITALS)

# Hospitals
HospitalProfiles = []
for h in HospitalIDs:
    name = f"{faker.last_name()} Hospital"
    city = pick(Cities)
    hp = {
        "HospitalID": h,
        "HospitalName": name,
        "HospitalLocation": city[1],
        "HospitalAddress": faker.address().replace("\n", ", "),
        "GovPrivate": random.choice(GovPrivate),
        "HospitalSpecialization": random.choice(HospitalSpecializationList)
    }
    HospitalProfiles.append(hp)
HospitalByID = {h["HospitalID"]: h for h in HospitalProfiles}

# Doctors (with their dimension IDs)
Doctors = []
for d in DoctorIDs:
    gender = pick(Gender)
    edu = pick(Education)
    spec = pick(Specialization)
    emp = pick(EmploymentType)
    sh = pick(Shift)
    st = pick(States)
    ct = pick(Cities)
    doc = {
        "DoctorID": d,
        "DoctorName": f"Dr. {faker.first_name()} {faker.last_name()}",
        "DoctorGender": gender,
        "DoctorEducation": edu,
        "DoctorSpecialization": spec,
        "DoctorEmploymentType": emp,
        "DoctorShift": sh,
        "DoctorState": st,
        "DoctorCity": ct,
        "DoctorYearsExperience": random.randint(1, 40),
        "DoctorPhone": faker.msisdn(),
        "DoctorEmail": faker.email()
    }
    Doctors.append(doc)
DoctorsByID = {doc["DoctorID"]: doc for doc in Doctors}

# Staff
Staff = []
for s in StaffIDs:
    gender = pick(Gender)
    role = pick(StaffRole)
    dept = pick(Department)
    sh = pick(Shift)
    emp = pick(EmploymentType)
    st = pick(States)
    ct = pick(Cities)
    stf = {
        "StaffID": s,
        "StaffFirstName": faker.first_name(),
        "StaffLastName": faker.last_name(),
        "StaffGender": gender,
        "StaffRole": role,
        "StaffDepartment": dept,
        "StaffShift": sh,
        "StaffEmploymentType": emp,
        "StaffState": st,
        "StaffCity": ct,
        "StaffYearsExperience": random.randint(0, 35),
        "StaffPhone": faker.msisdn(),
        "StaffEmail": faker.email()
    }
    Staff.append(stf)
StaffByID = {stf["StaffID"]: stf for stf in Staff}

# Create overlaps: some doctors are also staff (same person ID)
# We'll map some doctor IDs to staff IDs (making them the same person in both pools).
num_overlap_doctors = int(P_DOCTOR_ALSO_STAFF * UNIQUE_DOCTORS)
overlap_doctor_ids = random.sample(DoctorIDs, num_overlap_doctors)
# For simplicity make StaffIDs include those same IDs by creating new staff entries with the same ID prefix mapped.
# We'll create a mapping doctor_id -> staff_id (same string but added to StaffByID with combined attributes).
for docid in overlap_doctor_ids:
    # pick an existing staff to replace or create a new staff entry with same ID
    s_id = docid.replace("DOC", "STF") if docid.startswith("DOC") else "STF"+docid
    # If the s_id exists in StaffByID, overwrite with combined details; else add new
    doc = DoctorsByID[docid]
    combined = {
        "StaffID": s_id,
        "StaffFirstName": doc["DoctorName"].replace("Dr. ", "").split()[0],
        "StaffLastName": doc["DoctorName"].replace("Dr. ", "").split()[-1],
        "StaffGender": doc["DoctorGender"],
        "StaffRole": pick(StaffRole),
        "StaffDepartment": pick(Department),
        "StaffShift": doc["DoctorShift"],
        "StaffEmploymentType": doc["DoctorEmploymentType"],
        "StaffState": doc["DoctorState"],
        "StaffCity": doc["DoctorCity"],
        "StaffYearsExperience": doc["DoctorYearsExperience"],
        "StaffPhone": doc["DoctorPhone"],
        "StaffEmail": doc["DoctorEmail"]
    }
    # add/overwrite
    StaffByID[s_id] = combined
    Staff.append(combined)
    if s_id not in StaffIDs:
        StaffIDs.append(s_id)

# Medicines, Beds, Equipment
BedCatalog = []
for b in BedIDs:
    bt = pick(BedType)
    bs = pick(BedStatus)
    bc = {
        "BedID": b,
        "BedType": bt,
        "BedStatus": bs,
        "WardNumber": random.randint(1, 50),
        "RoomNumber": random.randint(1, 300)
    }
    BedCatalog.append(bc)
BedByID = {b["BedID"]: b for b in BedCatalog}

EquipCatalog = []
for e in EquipmentIDs:
    et = pick(EquipmentType)
    ec = pick(EquipmentCondition)
    dept = pick(Department)
    eq = {
        "EquipmentID": e,
        "EquipmentType": et,
        "EquipmentCondition": ec,
        "EquipmentManufacturer": faker.company(),
        "EquipmentSerialNumber": faker.bothify(text="SN-????-#####"),
        "EquipmentWarrantyExpiry": faker.date_between(start_date="+6m", end_date="+5y"),
        "EquipmentDepartment": dept,
        "EquipmentPrice": round(random.uniform(50000, 500000), 2)
    }
    EquipCatalog.append(eq)
EquipByID = {e["EquipmentID"]: e for e in EquipCatalog}

MedCatalog = []
for m in MedicineIDs:
    name = random.choice(["Paracetamol","Amoxicillin","Azithromycin","Ibuprofen","Metformin",
                          "Amlodipine","Omeprazole","Cough Syrup","Insulin Glargine","Vitamin D3"])
    usage = pick(MedicineUsage)
    ds = pick(DosageStrength)
    mtype = pick(MedicineType)
    colour = pick(MedicineColour)
    brand = pick(MedicineBrand)
    cat = pick(MedicineCategory)
    med = {
        "MedicineID": m,
        "MedicineName": name,
        "MedicineUsage": usage,
        "DosageStrength": ds,
        "MedicineType": mtype,
        "MedicineSizeML": random.choice([10, 20, 50, 100, 200]),
        "MedicineColour": colour,
        "MedicineBrand": brand,
        "MedicineCategory": cat,
        "MedicinePrice": round(random.uniform(5, 500), 2),
        "MedicineExpiryDate": faker.date_between(start_date="+3m", end_date="+2y"),
        "MedicineManufacturer": faker.company()
    }
    MedCatalog.append(med)
MedByID = {m["MedicineID"]: m for m in MedCatalog}

# Patient registry: create UNIQUE_PATIENTS patients with consistent demographics
PatientRegistry = {}
for pid in PatientIDs:
    fname = faker.first_name()
    lname = faker.last_name()
    gender = pick(Gender)
    st = pick(States)
    ct = pick(Cities)
    dob = faker.date_between(start_date="-90y", end_date="-1y")
    PatientRegistry[pid] = {
        "PatientID": pid,
        "PatientFirstName": fname,
        "PatientLastName": lname,
        "PatientFullName": f"{fname} {lname}",
        "PatientGender": gender,
        "PatientDOB": dob,
        "PatientState": st,
        "PatientCity": ct,
        "PatientPhone": faker.msisdn(),
        "PatientEmail": faker.email(),
        "PatientBloodGroup": random.choice(["A+","A-","B+","B-","AB+","AB-","O+","O-"]),
        "PatientAddress": faker.address().replace("\n", ", ")
    }

# ------------- Create visit distribution per patient (1..4 visits) to sum to TOTAL_ROWS -------------
visits_per_patient = {}
remaining = TOTAL_ROWS
# First assign 1 visit to everyone
for pid in PatientIDs:
    visits_per_patient[pid] = 1
    remaining -= 1
    if remaining <= 0:
        break

# Function to get random patient IDs for extra visits
patient_index = 0
pids_list = PatientIDs.copy()

# Distribute remaining visits, ensuring max 4 per patient
while remaining > 0:
    pid = pids_list[patient_index % len(pids_list)]
    if visits_per_patient.get(pid, 0) < 4:
        visits_per_patient[pid] = visits_per_patient.get(pid, 0) + 1
        remaining -= 1
    patient_index += 1
    # Shuffle occasionally to avoid long runs
    if patient_index % 10000 == 0:
        random.shuffle(pids_list)

# Build a flat list of (pid) repeated as per visits; we'll shuffle visits to randomize order
visit_patient_list = []
for pid, cnt in visits_per_patient.items():
    visit_patient_list.extend([pid] * cnt)

# Trim or extend if rounding issues occurred
if len(visit_patient_list) > TOTAL_ROWS:
    visit_patient_list = visit_patient_list[:TOTAL_ROWS]
elif len(visit_patient_list) < TOTAL_ROWS:
    # add more random patients up to TOTAL_ROWS (shouldn't often happen)
    while len(visit_patient_list) < TOTAL_ROWS:
        visit_patient_list.append(random.choice(PatientIDs))

random.shuffle(visit_patient_list)

# ------------- Visit date helper -------------
def choose_visit_dates(visit_seed=None):
    visit_date = random_date(START_DATE, END_DATE)
    is_admit = random.random() < P_ADMISSION
    if is_admit:
        admit_date = visit_date
        if random.random() < P_DISCHARGE_SAME_DAY:
            discharge_date = admit_date
        else:
            discharge_date = admit_date + timedelta(days=random.randint(1, 14))
    else:
        # OPD -> set admission and discharge to same day (user requested no blanks)
        admit_date = visit_date
        discharge_date = visit_date
    return visit_date, is_admit, admit_date, discharge_date

# ------------- FIELD ORDER (requested order with 35 ID columns included) -------------
FIELD_ORDER = [
    # Visit + Patient + Hospital block (IDs included)
    "VisitID","VisitDate","AdmissionDate","DischargeDate",
    "PatientID","PatientFullName","PatientGenderID","PatientGender","PatientDOB","PatientStateID","PatientState","PatientCityID","PatientCity","PatientPhone","PatientEmail","PatientBloodGroup","PatientAddress",
    "HospitalID","HospitalName","HospitalLocation","HospitalAddress","GovPrivate","HospitalSpecialization",

    # Doctor (IDs + descriptive)
    "DoctorID","DoctorName","DoctorGenderID","DoctorGender","DoctorEducationID","DoctorEducation","DoctorSpecializationID","DoctorSpecialization","DoctorEmploymentTypeID","DoctorEmploymentType","DoctorShiftID","DoctorShift","DoctorStateID","DoctorState","DoctorCityID","DoctorCity","DoctorYearsExperience","DoctorPhone","DoctorEmail","IsDoctorAlsoStaff",

    # Staff (IDs + descriptive)
    "StaffID","StaffFirstName","StaffLastName","StaffGenderID","StaffGender","StaffRoleID","StaffRole","StaffDepartmentID","StaffDepartment","StaffShiftID","StaffShift","StaffEmploymentTypeID","StaffEmploymentType","StaffStateID","StaffState","StaffCityID","StaffCity","StaffYearsExperience","StaffPhone","StaffEmail","IsStaffActingAsDoctor",

    # Visit-level Department (ID+label)
    "DepartmentID","Department",

    # Bed
    "BedID","BedTypeID","BedType","BedStatusID","BedStatus","WardNumber","RoomNumber",

    # Equipment
    "EquipmentID","EquipmentTypeID","EquipmentType","EquipmentConditionID","EquipmentCondition","EquipmentManufacturer","EquipmentSerialNumber","EquipmentWarrantyExpiry","EquipmentDepartmentID","EquipmentDepartment","EquipmentPrice",

    # Medicine (IDs + descriptive)
    "MedicineID","MedicineName","MedicineUsageID","MedicineUsage","DosageStrengthID","DosageStrength","MedicineTypeID","MedicineType","MedicineSizeML","MedicineColourID","MedicineColour","MedicineBrandID","MedicineBrand","MedicineCategoryID","MedicineCategory","MedicinePrice","MedicineExpiryDate","MedicineManufacturer",

    # Clinical & billing (IDs + descriptive)
    "VisitReasonID","VisitReason","DiagnosisID","Diagnosis","TestTypeID","TestType","InsuranceProviderID","InsuranceProvider","PaymentMethodID","PaymentMethod","PaymentStatusID","PaymentStatus","TotalAmount","PaidAmount"
]

# ------------- Generate & write CSV in chunks -------------
OUTPUT_FILE = "healthcare_500k_35ids.csv"

def generate_and_write():
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELD_ORDER, extrasaction="ignore")
        writer.writeheader()
        chunk = []
        rows_written = 0

        for idx in range(TOTAL_ROWS):
            visit_seq = idx + 1
            visit_id = f"VIS{str(visit_seq).zfill(8)}"

            pid = visit_patient_list[idx]
            p = PatientRegistry[pid]

            hosp_id = random.choice(HospitalIDs)
            hosp = HospitalByID[hosp_id]

            visit_dt, is_admit, admit_dt, discharge_dt = choose_visit_dates()

            # Choose doctor & staff (maybe overlap)
            doc_id = random.choice(DoctorIDs)
            # If this doctor was mapped to an overlapping staff above, we may produce staff_id accordingly
            # Attempt to find staff id that corresponds to doc (reverse mapping heuristic)
            possible_staff_id = doc_id.replace("DOC", "STF") if doc_id.startswith("DOC") else None

            # Decide if staff acts as doctor this visit
            staff_acting = 1 if random.random() < P_STAFF_ACTING_AS_DOCTOR else 0

            # Select staff id (may be same as doctor in some overlaps created earlier)
            if possible_staff_id and possible_staff_id in StaffByID and random.random() < 0.5:
                staff_id = possible_staff_id
            else:
                staff_id = random.choice(StaffIDs)

            # Is doctor also staff? if doc_id maps to existing staff id created earlier we mark flag
            is_doc_also_staff = 1 if (doc_id.replace("DOC", "STF") in StaffByID) else 0

            # For staff acting as doctor, optionally override doctor info with staff info (use same ID)
            if staff_acting:
                # set doctor to staff identity (so Staff acts as Doctor): create doctor-like view from staff
                if staff_id in StaffByID:
                    stf_map = StaffByID[staff_id]
                    # create a pseudo doctor representation
                    doc = {
                        "DoctorID": "DOC_FROM_" + staff_id,
                        "DoctorName": f"Dr. {stf_map.get('StaffFirstName','')} {stf_map.get('StaffLastName','')}",
                        "DoctorGender": stf_map.get("StaffGender", pick(Gender)),
                        "DoctorEducation": pick(Education),
                        "DoctorSpecialization": pick(Specialization),
                        "DoctorEmploymentType": stf_map.get("StaffEmploymentType", pick(EmploymentType)),
                        "DoctorShift": stf_map.get("StaffShift", pick(Shift)),
                        "DoctorState": stf_map.get("StaffState", pick(States)),
                        "DoctorCity": stf_map.get("StaffCity", pick(Cities)),
                        "DoctorYearsExperience": stf_map.get("StaffYearsExperience", random.randint(0,20)),
                        "DoctorPhone": stf_map.get("StaffPhone", faker.msisdn()),
                        "DoctorEmail": stf_map.get("StaffEmail", faker.email())
                    }
                else:
                    doc = DoctorsByID[doc_id]
            else:
                doc = DoctorsByID[doc_id]

            # Ensure staff object exists
            if staff_id in StaffByID:
                stf = StaffByID[staff_id]
            else:
                # fallback to random staff
                stf = random.choice(list(StaffByID.values()))

            # Visit-level department (may differ from staff/doctor)
            dept = pick(Department)

            # Bed / Equipment / Medicine selection
            bed = BedByID[random.choice(BedIDs)]
            eqp = EquipByID[random.choice(EquipmentIDs)]
            med = MedByID[random.choice(MedicineIDs)]

            # Clinical picks
            visit_reason = pick(VisitReason)

            # Diagnosis sometimes multiple
            if random.random() < P_MULTI_DIAGNOSIS:
                dxs = random.sample(Diagnosis, 2)
                diagnosis_name = f"{dxs[0][1]}, {dxs[1][1]}"
                diagnosis_id = f"{dxs[0][0]},{dxs[1][0]}"
            else:
                dx = pick(Diagnosis)
                diagnosis_name = dx[1]
                diagnosis_id = dx[0]

            test_type = pick(TestType)
            payment_method = pick(PaymentMethod)
            payment_status = pick(PaymentStatus)
            insurance = pick(InsuranceProvider)

            # Charges
            base_visit = random.randint(200, 2000)
            test_charge = random.randint(0, 6000) if random.random() < 0.55 else 0
            bed_charge = 0
            if is_admit:
                stay_days = 1 if discharge_dt == admit_dt else days_between(admit_dt, discharge_dt) + 1
                bed_type_id = bed["BedType"][0]
                rate_map = {1: 5000, 2: 2000, 3: 4000, 4: 3000, 5: 2500}
                bed_rate = rate_map.get(bed_type_id, 2000)
                bed_charge = stay_days * bed_rate
            medicine_charge = random.randint(50, 2000)
            total_amount = base_visit + test_charge + bed_charge + medicine_charge

            if payment_status[1] == "Paid":
                paid_amount = total_amount
            elif payment_status[1] == "Pending":
                paid_amount = random.randint(0, int(total_amount * 0.5))
            elif payment_status[1] == "Partially Paid":
                paid_amount = random.randint(int(total_amount * 0.4), int(total_amount * 0.9))
            else:
                paid_amount = 0

            # Build row dict
            row = {
                "VisitID": visit_id,
                "VisitDate": visit_dt.date().isoformat(),
                "AdmissionDate": admit_dt.date().isoformat(),
                "DischargeDate": discharge_dt.date().isoformat(),

                # patient + hospital
                "PatientID": p["PatientID"],
                "PatientFullName": p["PatientFullName"],
                "PatientGenderID": p["PatientGender"][0],
                "PatientGender": p["PatientGender"][1],
                "PatientDOB": p["PatientDOB"].isoformat(),
                "PatientStateID": p["PatientState"][0],
                "PatientState": p["PatientState"][1],
                "PatientCityID": p["PatientCity"][0],
                "PatientCity": p["PatientCity"][1],
                "PatientPhone": p["PatientPhone"],
                "PatientEmail": p["PatientEmail"],
                "PatientBloodGroup": p["PatientBloodGroup"],
                "PatientAddress": p["PatientAddress"],

                "HospitalID": hosp["HospitalID"],
                "HospitalName": hosp["HospitalName"],
                "HospitalLocation": hosp["HospitalLocation"],
                "HospitalAddress": hosp["HospitalAddress"],
                "GovPrivate": hosp["GovPrivate"],
                "HospitalSpecialization": hosp["HospitalSpecialization"],

                # doctor
                "DoctorID": doc["DoctorID"],
                "DoctorName": doc["DoctorName"],
                "DoctorGenderID": doc["DoctorGender"][0],
                "DoctorGender": doc["DoctorGender"][1],
                "DoctorEducationID": doc["DoctorEducation"][0],
                "DoctorEducation": doc["DoctorEducation"][1],
                "DoctorSpecializationID": doc["DoctorSpecialization"][0],
                "DoctorSpecialization": doc["DoctorSpecialization"][1],
                "DoctorEmploymentTypeID": doc["DoctorEmploymentType"][0],
                "DoctorEmploymentType": doc["DoctorEmploymentType"][1],
                "DoctorShiftID": doc["DoctorShift"][0],
                "DoctorShift": doc["DoctorShift"][1],
                "DoctorStateID": doc["DoctorState"][0],
                "DoctorState": doc["DoctorState"][1],
                "DoctorCityID": doc["DoctorCity"][0],
                "DoctorCity": doc["DoctorCity"][1],
                "DoctorYearsExperience": doc["DoctorYearsExperience"],
                "DoctorPhone": doc["DoctorPhone"],
                "DoctorEmail": doc["DoctorEmail"],
                "IsDoctorAlsoStaff": is_doc_also_staff,

                # staff
                "StaffID": stf["StaffID"],
                "StaffFirstName": stf["StaffFirstName"],
                "StaffLastName": stf["StaffLastName"],
                "StaffGenderID": stf["StaffGender"][0],
                "StaffGender": stf["StaffGender"][1],
                "StaffRoleID": stf["StaffRole"][0],
                "StaffRole": stf["StaffRole"][1],
                "StaffDepartmentID": stf["StaffDepartment"][0],
                "StaffDepartment": stf["StaffDepartment"][1],
                "StaffShiftID": stf["StaffShift"][0],
                "StaffShift": stf["StaffShift"][1],
                "StaffEmploymentTypeID": stf["StaffEmploymentType"][0],
                "StaffEmploymentType": stf["StaffEmploymentType"][1],
                "StaffStateID": stf["StaffState"][0],
                "StaffState": stf["StaffState"][1],
                "StaffCityID": stf["StaffCity"][0],
                "StaffCity": stf["StaffCity"][1],
                "StaffYearsExperience": stf["StaffYearsExperience"],
                "StaffPhone": stf["StaffPhone"],
                "StaffEmail": stf["StaffEmail"],
                "IsStaffActingAsDoctor": 1 if staff_acting else 0,

                # dept
                "DepartmentID": dept[0],
                "Department": dept[1],

                # bed
                "BedID": bed["BedID"],
                "BedTypeID": bed["BedType"][0],
                "BedType": bed["BedType"][1],
                "BedStatusID": bed["BedStatus"][0],
                "BedStatus": bed["BedStatus"][1],
                "WardNumber": bed["WardNumber"],
                "RoomNumber": bed["RoomNumber"],

                # equipment
                "EquipmentID": eqp["EquipmentID"],
                "EquipmentTypeID": eqp["EquipmentType"][0],
                "EquipmentType": eqp["EquipmentType"][1],
                "EquipmentConditionID": eqp["EquipmentCondition"][0],
                "EquipmentCondition": eqp["EquipmentCondition"][1],
                "EquipmentManufacturer": eqp["EquipmentManufacturer"],
                "EquipmentSerialNumber": eqp["EquipmentSerialNumber"],
                "EquipmentWarrantyExpiry": eqp["EquipmentWarrantyExpiry"].isoformat(),
                "EquipmentDepartmentID": eqp["EquipmentDepartment"][0],
                "EquipmentDepartment": eqp["EquipmentDepartment"][1],
                "EquipmentPrice": eqp["EquipmentPrice"],

                # medicine
                "MedicineID": med["MedicineID"],
                "MedicineName": med["MedicineName"],
                "MedicineUsageID": med["MedicineUsage"][0],
                "MedicineUsage": med["MedicineUsage"][1],
                "DosageStrengthID": med["DosageStrength"][0],
                "DosageStrength": med["DosageStrength"][1],
                "MedicineTypeID": med["MedicineType"][0],
                "MedicineType": med["MedicineType"][1],
                "MedicineSizeML": med["MedicineSizeML"],
                "MedicineColourID": med["MedicineColour"][0],
                "MedicineColour": med["MedicineColour"][1],
                "MedicineBrandID": med["MedicineBrand"][0],
                "MedicineBrand": med["MedicineBrand"][1],
                "MedicineCategoryID": med["MedicineCategory"][0],
                "MedicineCategory": med["MedicineCategory"][1],
                "MedicinePrice": med["MedicinePrice"],
                "MedicineExpiryDate": med["MedicineExpiryDate"].isoformat(),
                "MedicineManufacturer": med["MedicineManufacturer"],

                # clinical & billing
                "VisitReasonID": visit_reason[0],
                "VisitReason": visit_reason[1],
                "DiagnosisID": diagnosis_id,
                "Diagnosis": diagnosis_name,
                "TestTypeID": test_type[0],
                "TestType": test_type[1],
                "InsuranceProviderID": insurance[0],
                "InsuranceProvider": insurance[1],
                "PaymentMethodID": payment_method[0],
                "PaymentMethod": payment_method[1],
                "PaymentStatusID": payment_status[0],
                "PaymentStatus": payment_status[1],
                "TotalAmount": total_amount,
                "PaidAmount": paid_amount
            }

            chunk.append(row)
            rows_written += 1

            if len(chunk) >= CHUNK_SIZE:
                writer.writerows(chunk)
                print(f"➡️ Written {rows_written:,} rows so far...")
                chunk = []

        # final flush
        if chunk:
            writer.writerows(chunk)
            print(f"➡️ Written {rows_written:,} rows (final).")

    print(f"✅ Done. Wrote {rows_written:,} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_and_write()
