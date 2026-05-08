"""
demo_setup.py — BFAM Sales Intelligence Platform v2
────────────────────────────────────────────────────
Run once before starting the demo.
Seeds all 337 accounts (3 COE + 15 RBH + 58 CBH + 261 BIC)
and loads your 6 Excel files into MySQL.

Usage:
  cd backend
  python demo_setup.py
"""

import sys
from pathlib import Path

print("""
╔══════════════════════════════════════════════════════╗
║  BFAM Sales Intelligence Platform — Demo Setup v2  ║
║  Bajaj Finserv Asset Management · Partners COE      ║
╚══════════════════════════════════════════════════════╝
""")

if not Path("app.py").exists():
    print("ERROR: Run from inside the backend folder: cd backend")
    sys.exit(1)

for d in ["data/raw","data/processed","logs"]:
    Path(d).mkdir(parents=True, exist_ok=True)

from dotenv import load_dotenv
load_dotenv()

print("Loading modules...")
try:
    from database.db import init_db, SessionLocal, User, Season, GamificationConfig
    from passlib.context import CryptContext
    pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
    def hp(p): return pwd_ctx.hash(p)
except ImportError as e:
    print(f"Import error: {e}\nRun: pip install -r requirements.txt")
    sys.exit(1)

# ── Step 1: DB tables ─────────────────────────────────
print("\n[1/5] Creating MySQL tables...")
try:
    init_db()
    print("  ✓ All tables created")
except Exception as e:
    print(f"  ✗ Database error: {e}")
    print("  Check your .env file: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
    sys.exit(1)

db = SessionLocal()

# ── Step 2: Season ────────────────────────────────────
print("\n[2/5] Setting up FY 2025-26 season...")
if not db.query(Season).first():
    db.add(Season(name="FY 2025-26", start_date="2025-04-01", is_active=True))
    db.commit()
    print("  ✓ Season created")
else:
    print("  ✓ Season already exists")

# ── Step 3: Gamification config ───────────────────────
print("\n[3/5] Seeding gamification config...")
if not db.query(GamificationConfig).first():
    db.add(GamificationConfig(
        pts_per_txn=3, pts_per_activation=15, pts_per_50k_inflow=1,
        streak_multiplier_days=7, streak_multiplier_value=1.5,
        module_bonus={"po3":5,"wsip":12,"savings":10,"wa":0,"sip":0},
        challenges=[
            {"id":"sip5","name":"Daily SIP Sprint","desc":"Register 5 SIPs today",
             "target":5,"metric":"sip_count","bonus":10,"color":"#1565C0","active":True},
            {"id":"act5","name":"Activation Ace","desc":"Activate 5 partners today",
             "target":5,"metric":"activation","bonus":20,"color":"#6B3FA0","active":True},
            {"id":"lakh","name":"Lakh Club","desc":"₹1L+ inflows today",
             "target":100000,"metric":"inflows","bonus":15,"color":"#0B9F6C","active":True},
            {"id":"p3","name":"Power of 3 Pro","desc":"3 P3 transactions today",
             "target":3,"metric":"pad3","bonus":25,"color":"#E6A817","active":True},
            {"id":"wsip1","name":"Wealth SIP Star","desc":"1 Wealth SIP today",
             "target":1,"metric":"wsip","bonus":30,"color":"#D62B2B","active":True},
        ],
        announcements=[
            {"id":1,"text":"Welcome to BFAM Sales Intelligence Platform!","date":"2026-04-27","active":True},
            {"id":2,"text":"Power of 3 Pro challenge is now live — earn 25 bonus points!","date":"2026-04-27","active":True},
        ]
    ))
    db.commit()
    print("  ✓ Config seeded")
else:
    print("  ✓ Config already exists")

# ── Step 4: Create all 337 user accounts ──────────────
print("\n[4/5] Creating user accounts...")

# COE Admins
COE_USERS = [
    ("ADMIN01", "COE Admin 1",     "COE", "Admin@123"),
    ("ADMIN02", "COE Admin 2",     "COE", "Admin@123"),
    ("ADMIN03", "COE Admin 3",     "COE", "Admin@123"),
]

# RBH — one per region
RBH_USERS = [
    ("RBH01", "RBH - Andhra Pradesh, Telangana",                    "RBH", "Welcome@123", "Andhra Pradesh, Telangana"),
    ("RBH02", "RBH - Bihar, Jharkhand, Orissa, Chattisgarh",        "RBH", "Welcome@123", "Bihar, Jharkhand, Orissa, Chattisgarh"),
    ("RBH03", "RBH - Delhi, NCR",                                    "RBH", "Welcome@123", "Delhi, NCR"),
    ("RBH04", "RBH - Gujarat",                                       "RBH", "Welcome@123", "Gujarat"),
    ("RBH05", "RBH - Karnataka",                                     "RBH", "Welcome@123", "Karnataka"),
    ("RBH06", "RBH - Kerala",                                        "RBH", "Welcome@123", "Kerala"),
    ("RBH07", "RBH - Kolkata",                                       "RBH", "Welcome@123", "Kolkata"),
    ("RBH08", "RBH - Madhya Pradesh",                                "RBH", "Welcome@123", "Madhya Pradesh"),
    ("RBH09", "RBH - Mumbai",                                        "RBH", "Welcome@123", "Mumbai"),
    ("RBH10", "RBH - Punjab, Haryana, Himachal Pradesh, J&K",       "RBH", "Welcome@123", "Punjab, Haryana, Himachal Pradesh, Jammu Kashmir"),
    ("RBH11", "RBH - Rajasthan",                                     "RBH", "Welcome@123", "Rajasthan"),
    ("RBH12", "RBH - Rest of Bengal, North East",                   "RBH", "Welcome@123", "Rest of Bengal, North East"),
    ("RBH13", "RBH - Rest of Maharashtra, Goa",                     "RBH", "Welcome@123", "Rest of Maharashtra, Goa"),
    ("RBH14", "RBH - Tamil Nadu",                                    "RBH", "Welcome@123", "Tamil Nadu"),
    ("RBH15", "RBH - Uttar Pradesh, Uttarakhand",                   "RBH", "Welcome@123", "Uttar Pradesh, Uttarakhand"),
]

# CBH — 58 cluster managers
# Format: (login_code, name, real_bic_emp, has_bic_data, regions)
# login_code = real emp code if has_bic_data=True, else CBHxx
CBH_USERS = [
    ("75102",  "Aashish Rohatgi",              "75102",  True,  "Bihar, Jharkhand, Orissa, Chattisgarh",                "Jharkhand"),
    ("CBH02",  "Amit Kumar01",                 None,     False, "Bihar, Jharkhand, Orissa, Chattisgarh",                "Patna"),
    ("CBH03",  "Anantharaman S",               None,     False, "Karnataka",                                            "South Karnataka"),
    ("75187",  "Ankur Gupta",                  "75187",  True,  "Uttar Pradesh, Uttarakhand",                           "Kanpur"),
    ("75613",  "Anshul Rustagi",               "75613",  True,  "Delhi, NCR",                                           "NCR-MFD"),
    ("CBH06",  "Anup Pathak",                  None,     False, "Rest of Maharashtra, Goa",                             "Pune"),
    ("CBH07",  "Deep Jyoti Paul",              None,     False, "Rest of Bengal, North East",                           "North Bengal"),
    ("CBH08",  "Dileen Bharathan",             None,     False, "Tamil Nadu",                                           "Chennai"),
    ("75637",  "Duraisamy K",                  "75637",  True,  "Tamil Nadu",                                           "Chennai"),
    ("75210",  "G Arunachalam",                "75210",  True,  "Bihar, Jharkhand, Orissa, Chattisgarh",                "Chattisgarh"),
    ("75073",  "Gaurav Jain",                  "75073",  True,  "Mumbai",                                               "Mumbai-MFD"),
    ("75457",  "Gaurav Mathur",                "75457",  True,  "Rajasthan",                                            "Jaipur"),
    ("75197",  "Girish Kumar Mantada",         "75197",  True,  "Andhra Pradesh, Telangana",                            "Vijaywada"),
    ("CBH14",  "Haresh S",                     None,     False, "Bihar, Jharkhand, Orissa, Chattisgarh",                "Patna"),
    ("75314",  "Hemanth Kumar",                "75314",  True,  "Karnataka",                                            "Bangalore-1"),
    ("75176",  "Indranil Mukherjee",           "75176",  True,  "Rest of Bengal, North East",                           "South Bengal"),
    ("CBH17",  "Kunal Kharade",                None,     False, "Gujarat",                                              "Ahmedabad"),
    ("75829",  "Lakhan Sharma",                "75829",  True,  "Rajasthan",                                            "Jodhpur"),
    ("75087",  "Loganathan C",                 "75087",  True,  "Tamil Nadu",                                           "Coimbatore"),
    ("75302",  "Milind Kamble",                "75302",  True,  "Rest of Maharashtra, Goa",                             "Pune"),
    ("75617",  "Mithun Divakar N",             "75617",  True,  "Karnataka",                                            "Bangalore-2"),
    ("CBH22",  "Mohammad Kamran",              None,     False, "Uttar Pradesh, Uttarakhand",                           "Lucknow"),
    ("75731",  "Mrinal Monty",                 "75731",  True,  "Kolkata",                                              "Kolkata"),
    ("75491",  "Naveen Raj G",                 "75491",  True,  "Karnataka",                                            "South Karnataka"),
    ("CBH25",  "Nidhi Mittal",                 None,     False, "Madhya Pradesh",                                       "Indore"),
    ("75164",  "Nikhil Ganjoo",                "75164",  True,  "Punjab, Haryana, Himachal Pradesh, Jammu Kashmir",     "Jammu & Kashmir"),
    ("75532",  "Noor Mohammad Mukhtar Shaikh", "75532",  True,  "Rest of Maharashtra, Goa",                             "Nasik"),
    ("75200",  "Prakash Saxena",               "75200",  True,  "Uttar Pradesh, Uttarakhand",                           "Agra"),
    ("75126",  "Pranav Parikh",                "75126",  True,  "Gujarat",                                              "Ahmedabad"),
    ("CBH30",  "Prasanna Kumar S",             None,     False, "Karnataka",                                            "Bangalore-2"),
    ("CBH31",  "Prateek Mathur",               None,     False, "Madhya Pradesh",                                       "Bhopal"),
    ("75315",  "Pratik Sawant",                "75315",  True,  "Gujarat",                                              "Vadodara"),
    ("CBH33",  "Puneet Pal Jindal",            None,     False, "Delhi, NCR",                                           "NCR-MFD"),
    ("75252",  "Raghav Sukhadia",              "75252",  True,  "Gujarat",                                              "Surat"),
    ("CBH35",  "Rahul Bind",                   None,     False, "Rest of Maharashtra, Goa",                             "Nagpur"),
    ("75032",  "Raj Singh",                    "75032",  True,  "Bihar, Jharkhand, Orissa, Chattisgarh",                "Jharkhand"),
    ("CBH37",  "Rajesh Manaktala",             None,     False, "Uttar Pradesh, Uttarakhand",                           "Kanpur"),
    ("75482",  "Rajiv Kumar Singh",            "75482",  True,  "Rest of Bengal, North East",                           "North East Cluster"),
    ("75845",  "Ranjith J S",                  "75845",  True,  "Kerala",                                               "Kerala"),
    ("75389",  "Ravindra Padghan",             "75389",  True,  "Rest of Maharashtra, Goa",                             "Aurangabad"),
    ("75867",  "Rushabhsen Kothari",           "75867",  True,  "Gujarat",                                              "Ahmedabad"),
    ("75166",  "Sandeep Saxena",               "75166",  True,  "Uttar Pradesh, Uttarakhand",                           "Moradabad"),
    ("CBH43",  "Satish Nachinolkar",           None,     False, "Rest of Maharashtra, Goa",                             "Goa"),
    ("75178",  "Satyam Katyayan",              "75178",  True,  "Bihar, Jharkhand, Orissa, Chattisgarh",                "Orissa"),
    ("75243",  "Saurabh Aggarwal",             "75243",  True,  "Punjab, Haryana, Himachal Pradesh, Jammu Kashmir",     "Haryana"),
    ("75152",  "Shailesh Kumar",               "75152",  True,  "Bihar, Jharkhand, Orissa, Chattisgarh",                "Rest Of Bihar"),
    ("CBH47",  "Shashank Bharadwaj",           None,     False, "Punjab, Haryana, Himachal Pradesh, Jammu Kashmir",     "Chandigarh"),
    ("75695",  "Sorabh Chugh",                 "75695",  True,  "Punjab, Haryana, Himachal Pradesh, Jammu Kashmir",     "Chandigarh"),
    ("CBH49",  "Sujay Ghoshal",                None,     False, "Andhra Pradesh, Telangana",                            "Hyderabad"),
    ("75128",  "Sumit Adesara",                "75128",  True,  "Gujarat",                                              "Rajkot"),
    ("75039",  "Suneet Puri",                  "75039",  True,  "Punjab, Haryana, Himachal Pradesh, Jammu Kashmir",     "Chandigarh"),
    ("75492",  "Suraj Mishra01",               "75492",  True,  "Uttar Pradesh, Uttarakhand",                           "Varanasi"),
    ("75240",  "Suresh Balaji R",              "75240",  True,  "Tamil Nadu",                                           "Chennai"),
    ("CBH54",  "Upasana Ray",                  None,     False, "Mumbai",                                               "Mumbai-Bank"),
    ("CBH55",  "Varun Misser",                 None,     False, "Mumbai",                                               "Mumbai-MFD"),
    ("75476",  "Vijayarajan B",                "75476",  True,  "Tamil Nadu",                                           "Trichy"),
    ("75434",  "Vinay Sharma",                 "75434",  True,  "Uttar Pradesh, Uttarakhand",                           "Dehradun"),
    ("75163",  "Vishal Tiwari",                "75163",  True,  "Punjab, Haryana, Himachal Pradesh, Jammu Kashmir",     "Jalandhar"),
]

# BIC accounts — all 261 from the files
BIC_USERS = [
    ("75752","Aakanksha Ram","Rest of Maharashtra, Goa","Pune"),
    ("75102","Aashish Rohatgi","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75311","Abhay Bhansali","Mumbai","Mumbai-MFD"),
    ("75884","Abhay Pradeep Shedge","Mumbai","Mumbai-MFD"),
    ("75245","Abhishek Kapadia","Gujarat","Ahmedabad"),
    ("75477","Abhishek Kumar","Bihar, Jharkhand, Orissa, Chattisgarh","Patna"),
    ("75189","Abhishek Pal","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Haryana"),
    ("75710","Adarsh Chauhan","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jalandhar"),
    ("75317","Aditya Paidalwar","Rest of Maharashtra, Goa","Nagpur"),
    ("75729","Ajinkya Dakhane","Rest of Maharashtra, Goa","Aurangabad"),
    ("75448","Ajmal Irfan","Kerala","Kerala"),
    ("75450","Akanksha Gupta","Delhi, NCR","NCR-MFD"),
    ("75489","Akash Gajanan Gite","Rest of Maharashtra, Goa","Nagpur"),
    ("75852","Akshara E S","Karnataka","Bangalore-2"),
    ("75279","Aman Panchal","",""),
    ("75612","Aman Verma","Uttar Pradesh, Uttarakhand","Moradabad"),
    ("75260","Amartya Ranjan Prusty","Andhra Pradesh, Telangana","Hyderabad"),
    ("75124","Amit Kumar","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75103","Amit Singh Bisht","Delhi, NCR","NCR-MFD"),
    ("75364","Amol Nimba Suryawanshi","Rest of Maharashtra, Goa","Nasik"),
    ("75838","Anand Tankariya","Gujarat","Rajkot"),
    ("75196","Aniruddha Shukla","",""),
    ("75171","Ankit Gaikwad","Madhya Pradesh","Indore"),
    ("75187","Ankur Gupta","Uttar Pradesh, Uttarakhand","Kanpur"),
    ("75514","Anshul Nandwana","Rest of Maharashtra, Goa","Pune"),
    ("75613","Anshul Rustagi","Delhi, NCR","NCR-MFD"),
    ("75766","Anupam Saha","Kolkata","Kolkata"),
    ("75097","Anupol Saikia","Rest of Bengal, North East","North East Cluster"),
    ("75112","Anurag Dasgupta","Rest of Bengal, North East","South Bengal"),
    ("75320","Arindam Das Mahapatra","Bihar, Jharkhand, Orissa, Chattisgarh","Orissa"),
    ("75392","Ariz Muzaffar","Bihar, Jharkhand, Orissa, Chattisgarh","Rest Of Bihar"),
    ("75106","Arpan Shukla","Bihar, Jharkhand, Orissa, Chattisgarh","Chattisgarh"),
    ("75144","Arpita Sharma","Rajasthan","Jaipur"),
    ("75880","Aruna Vikram Karpe","Rest of Maharashtra, Goa","Nasik"),
    ("75105","Arvind Sengar","Uttar Pradesh, Uttarakhand","Kanpur"),
    ("75306","Ashish Mohan Sinha","Bihar, Jharkhand, Orissa, Chattisgarh","Rest Of Bihar"),
    ("75385","Ashishkumar Shrimali","Gujarat","Vadodara"),
    ("75133","Ashutosh Singh","Uttar Pradesh, Uttarakhand","Lucknow"),
    ("75435","Ashwani Kumar","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jalandhar"),
    ("75626","Ashwin S","Tamil Nadu","Chennai"),
    ("75508","Atul Kumar Sahu","Mumbai","Mumbai-Bank"),
    ("75546","Ayush Wadel","","Virtual"),
    ("75741","B Shreyas Prabhu","Rest of Maharashtra, Goa","Pune"),
    ("75831","Beauty Kumari","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75368","Bhim Prasad","Uttar Pradesh, Uttarakhand","Kanpur"),
    ("75470","Bibin Peter","Kerala","Kerala"),
    ("75656","Bishal Sen","Rest of Bengal, North East","North East Cluster"),
    ("75180","Chandan Kumar","Bihar, Jharkhand, Orissa, Chattisgarh","Patna"),
    ("75635","Cilambarasu Ramasamy","Tamil Nadu","Coimbatore"),
    ("75803","Darshi Hingrajia","Gujarat","Ahmedabad"),
    ("75256","Deepak Khajuria","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jammu & Kashmir"),
    ("75794","Dev Jani","Gujarat","Rajkot"),
    ("75701","Devaraj Hosagoudara","Karnataka","North Karnataka"),
    ("75226","Devashish Wanve","Rest of Maharashtra, Goa","Nagpur"),
    ("75767","Dhaval Jethva","Gujarat","Rajkot"),
    ("75091","Dibyendu Das","Kolkata","Kolkata"),
    ("75808","Digvijay Singh","Uttar Pradesh, Uttarakhand","Dehradun"),
    ("75420","Diksha Gaur","Delhi, NCR","NCR-MFD"),
    ("75637","Duraisamy K","Tamil Nadu","Chennai"),
    ("75210","G Arunachalam","Bihar, Jharkhand, Orissa, Chattisgarh","Chattisgarh"),
    ("75344","Ganesh Pechfule","Rest of Maharashtra, Goa","Aurangabad"),
    ("75073","Gaurav Jain","Mumbai","Mumbai-MFD"),
    ("75090","Gaurav Kumar Golu","Uttar Pradesh, Uttarakhand","Varanasi"),
    ("75457","Gaurav Mathur","Rajasthan","Jaipur"),
    ("75698","Gaurav Mishra","Bihar, Jharkhand, Orissa, Chattisgarh","Rest Of Bihar"),
    ("75127","Gautam Jha","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Haryana"),
    ("75197","Girish Kumar Mantada","Andhra Pradesh, Telangana","Vijaywada"),
    ("75727","Gourav Sharma","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Ludhiana"),
    ("75203","Govind Rajput","Delhi, NCR","NCR-MFD"),
    ("75454","Harsha J G","Karnataka","Bangalore-1"),
    ("75790","Harshit Parmarthi","Madhya Pradesh","Indore"),
    ("75314","Hemanth Kumar","Karnataka","Bangalore-1"),
    ("75627","Hrishabh Sharma","Madhya Pradesh","Bhopal"),
    ("75176","Indranil Mukherjee","Rest of Bengal, North East","South Bengal"),
    ("75262","Ira Gupta","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Chandigarh"),
    ("75377","Jagjeet Singh","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Chandigarh"),
    ("75177","Jagtar Singh","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jalandhar"),
    ("75483","Jai Hiralal Lund","Mumbai","Mumbai-MFD"),
    ("75811","Jaidev Giri","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Haryana"),
    ("75712","Janhavi Ajgaonkar","Mumbai","Mumbai-MFD"),
    ("75847","Jatin Saini","Rajasthan","Jaipur"),
    ("75672","Jeet Palan","Gujarat","Rajkot"),
    ("75891","Jegan Sargunam","Tamil Nadu","Trichy"),
    ("75570","Jigar Joshi","Gujarat","Rajkot"),
    ("75588","Joel K","Tamil Nadu","Trichy"),
    ("75044","Jyoti Modak","Rest of Bengal, North East","North Bengal"),
    ("75773","KRUNAL MER","Gujarat","Vadodara"),
    ("75436","Kamleshbhai Parmar","Gujarat","Vadodara"),
    ("75475","Karishma Thakur","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Ludhiana"),
    ("75616","Kartik Bajpai","Uttar Pradesh, Uttarakhand","Kanpur"),
    ("75648","Kartik Goundar","Karnataka","South Karnataka"),
    ("75451","Kiran B","Karnataka","Bangalore-1"),
    ("75574","Kowtham Babu","Tamil Nadu","Coimbatore"),
    ("75860","Kranthi Reddy","Andhra Pradesh, Telangana","Vijaywada"),
    ("75642","Kumar Vardhan","Uttar Pradesh, Uttarakhand","Moradabad"),
    ("75294","Kundan Sharma","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75829","Lakhan Sharma","Rajasthan","Jodhpur"),
    ("75087","Loganathan C","Tamil Nadu","Coimbatore"),
    ("75563","Lokesh Kumar R","Tamil Nadu","Chennai"),
    ("75509","Maharnab Talukdar","Kolkata","Kolkata"),
    ("75771","Mallikarjun Angadi","Karnataka","North Karnataka"),
    ("75060","Md Ahashan Alam","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75302","Milind Kamble","Rest of Maharashtra, Goa","Pune"),
    ("75617","Mithun Divakar N","Karnataka","Bangalore-2"),
    ("75290","Mohit Yadav","Uttar Pradesh, Uttarakhand","Lucknow"),
    ("75726","Mridhul Np","Kerala","Kerala"),
    ("75731","Mrinal Monty","Kolkata","Kolkata"),
    ("75158","Mukesh Kumar Singh","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75598","Naimish Sojitra","Gujarat","Rajkot"),
    ("75366","Nain Mewari","Uttar Pradesh, Uttarakhand","Moradabad"),
    ("75136","Namrata Ambre","Mumbai","Mumbai-MFD"),
    ("75595","Nand Bihari","Rajasthan","Jaipur"),
    ("75862","Nandhakumar S","Tamil Nadu","Chennai"),
    ("75835","Nandini Gupta","Uttar Pradesh, Uttarakhand","Kanpur"),
    ("75569","Narayana Kamalakar","Andhra Pradesh, Telangana","Vijaywada"),
    ("75491","Naveen Raj G","Karnataka","South Karnataka"),
    ("75164","Nikhil Ganjoo","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jammu & Kashmir"),
    ("75658","Nikhil Kurade","Rest of Maharashtra, Goa","Kolhapur"),
    ("75455","Nisha Gupta","Bihar, Jharkhand, Orissa, Chattisgarh","Chattisgarh"),
    ("75515","Nitin Shukla","Rest of Maharashtra, Goa","Pune"),
    ("75532","Noor Mohammad Mukhtar Shaikh","Rest of Maharashtra, Goa","Nasik"),
    ("75220","Pankaj Bramhadeo Dubey","Mumbai","Mumbai-MFD"),
    ("75061","Pankaj Sharma","Uttar Pradesh, Uttarakhand","Agra"),
    ("75633","Paras Bhatt","Gujarat","Rajkot"),
    ("75499","Parth Bhaskarbhai Vashi","Gujarat","Surat"),
    ("75494","Parthiban S","Tamil Nadu","Trichy"),
    ("75573","Pavitra Mehta","Gujarat","Surat"),
    ("75535","Phanikumar C","Karnataka","North Karnataka"),
    ("75276","Prachi Dharmesh Sheth","Gujarat","Ahmedabad"),
    ("75200","Prakash Saxena","Uttar Pradesh, Uttarakhand","Agra"),
    ("75643","Prakhar Mulmuley","Rest of Maharashtra, Goa","Pune"),
    ("75126","Pranav Parikh","Gujarat","Ahmedabad"),
    ("75301","Pranjal Gupta","Uttar Pradesh, Uttarakhand","Agra"),
    ("75827","Prasanna Moorthy R","Tamil Nadu","Chennai"),
    ("75063","Prashubh S","Uttar Pradesh, Uttarakhand","Varanasi"),
    ("75444","Prathap S","Karnataka","South Karnataka"),
    ("75315","Pratik Sawant","Gujarat","Vadodara"),
    ("75819","Pratik Thummar","Gujarat","Surat"),
    ("75756","Prerna Ahuja","Rajasthan","Jaipur"),
    ("75618","Priti Vivek Shah","Rest of Maharashtra, Goa","Nagpur"),
    ("75678","Priya Choudhury","Rest of Bengal, North East","North Bengal"),
    ("75045","Priyansh Shah","Gujarat","Vadodara"),
    ("75410","Puja Raj","Bihar, Jharkhand, Orissa, Chattisgarh","Orissa"),
    ("75384","Rafiq Khan","Uttar Pradesh, Uttarakhand","Agra"),
    ("75252","Raghav Sukhadia","Gujarat","Surat"),
    ("75321","Rahul Das","Rest of Bengal, North East","South Bengal"),
    ("75115","Rahul Singh","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Ludhiana"),
    ("75813","Raj Kumar Sen","Madhya Pradesh","Bhopal"),
    ("75032","Raj Singh","Bihar, Jharkhand, Orissa, Chattisgarh","Patna"),
    ("75341","Rajesh R","Kerala","Kerala"),
    ("75482","Rajiv Kumar Singh","Rest of Bengal, North East","North East Cluster"),
    ("75241","Rajkumar Ganesan","Tamil Nadu","Trichy"),
    ("75464","Rakesh Brahmbhatt","Gujarat","Rajkot"),
    ("75699","Rakesh Mohapatra","Bihar, Jharkhand, Orissa, Chattisgarh","Orissa"),
    ("75845","Ranjith J S","Kerala","Kerala"),
    ("75817","Rashmi Rout","Bihar, Jharkhand, Orissa, Chattisgarh","Orissa"),
    ("75389","Ravindra Padghan","Rest of Maharashtra, Goa","Aurangabad"),
    ("75833","Raviraj Nathawat","Rajasthan","Jaipur"),
    ("75161","Rishabh Jain","Uttar Pradesh, Uttarakhand","Moradabad"),
    ("75620","Ritu Singh","Mumbai","Mumbai-MFD"),
    ("75826","Rohan Kadam","Mumbai","Mumbai-Bank"),
    ("75566","Rohit Gupta","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Haryana"),
    ("75763","Rohit Sharma","Rest of Bengal, North East","North East Cluster"),
    ("75867","Rushabhsen Kothari","Gujarat","Ahmedabad"),
    ("75318","S Kumaran","Tamil Nadu","Chennai"),
    ("75630","S Sainath","Andhra Pradesh, Telangana","Vishakhapatnam"),
    ("75581","Sachin Pachauri","Uttar Pradesh, Uttarakhand","Agra"),
    ("75182","Sajal Shukla","Bihar, Jharkhand, Orissa, Chattisgarh","Chattisgarh"),
    ("75857","Sajan M D","Karnataka","South Karnataka"),
    ("75159","Sandeep Kolap","Rest of Maharashtra, Goa","Kolhapur"),
    ("75166","Sandeep Saxena","Uttar Pradesh, Uttarakhand","Moradabad"),
    ("75842","Sandip Singh","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jalandhar"),
    ("75209","Sanjay Kumar Pandit","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75859","Sanjib Kumar Nepak","Bihar, Jharkhand, Orissa, Chattisgarh","Orissa"),
    ("75806","Santosh Yadav","Uttar Pradesh, Uttarakhand","Varanasi"),
    ("75394","Santu Chakraborty","Rest of Bengal, North East","South Bengal"),
    ("75789","Sapna Kumari","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Chandigarh"),
    ("75873","Sarath M S","Kerala","Kerala"),
    ("75609","Satheeshkumar Rajendran","Tamil Nadu","Trichy"),
    ("75178","Satyam Katyayan","Bihar, Jharkhand, Orissa, Chattisgarh","Orissa"),
    ("75243","Saurabh Aggarwal","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Haryana"),
    ("75662","Sayantan Banerjee","Rest of Bengal, North East","North Bengal"),
    ("75749","Sejal Maheshwari","Rest of Maharashtra, Goa","Nagpur"),
    ("75567","Senthilkumar Kesavan","Tamil Nadu","Coimbatore"),
    ("75152","Shailesh Kumar","Bihar, Jharkhand, Orissa, Chattisgarh","Rest Of Bihar"),
    ("75705","Shashank Pratap Singh","Uttar Pradesh, Uttarakhand","Varanasi"),
    ("75082","Shivaji Singh","Uttar Pradesh, Uttarakhand","Lucknow"),
    ("75350","Shubham Gupta01","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Chandigarh"),
    ("75788","Shubham Jain","Delhi, NCR","NCR-MFD"),
    ("75762","Shubham Kumar","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75169","Shubham Mesare","Rest of Maharashtra, Goa","Pune"),
    ("75316","Shubham Pramod Naik","Rest of Maharashtra, Goa","Goa"),
    ("75608","Shubham Priya","Bihar, Jharkhand, Orissa, Chattisgarh","Patna"),
    ("75523","Shubham Rajguru","Gujarat","Ahmedabad"),
    ("75785","Shweta Jaiswal","Uttar Pradesh, Uttarakhand","Varanasi"),
    ("75733","Siddharth Panchal","Mumbai","Mumbai-MFD"),
    ("75759","Sima Mistri","Kolkata","Kolkata"),
    ("75772","Simran Narang","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Chandigarh"),
    ("75723","Siva Gnanabalan","Tamil Nadu","Chennai"),
    ("75782","Smitgiri Goswami","Gujarat","Surat"),
    ("75780","Somali Bhattacharjee","Kolkata","Kolkata"),
    ("75120","Someswarao Sanapathi","Andhra Pradesh, Telangana","Vishakhapatnam"),
    ("75753","Somnath Ghosh","Kolkata","Kolkata"),
    ("75155","Sonalika Tank","Gujarat","Ahmedabad"),
    ("75695","Sorabh Chugh","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Chandigarh"),
    ("75572","Soumya Ranjan Panda","Bihar, Jharkhand, Orissa, Chattisgarh","Orissa"),
    ("75746","Sowmya Nagaraj","Tamil Nadu","Trichy"),
    ("75743","Sreenath M Nampoothiri","Kerala","Kerala"),
    ("75393","Subhajit Banerjee","Rest of Bengal, North East","South Bengal"),
    ("75555","Subhasmita Barik","Bihar, Jharkhand, Orissa, Chattisgarh","Orissa"),
    ("75640","Sudeep Chatterjee","Rest of Bengal, North East","South Bengal"),
    ("75467","Sujay Ghoshal","Andhra Pradesh, Telangana","Hyderabad"),
    ("75568","SujithKumar Balasani","Andhra Pradesh, Telangana","Hyderabad"),
    ("75128","Sumit Adesara","Gujarat","Rajkot"),
    ("75304","Sumita Kumari","Bihar, Jharkhand, Orissa, Chattisgarh","Patna"),
    ("75039","Suneet Puri","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Chandigarh"),
    ("75552","Sunil Kumar Choudhary","Bihar, Jharkhand, Orissa, Chattisgarh","Rest Of Bihar"),
    ("75529","Supriyo Dey","Kolkata","Kolkata"),
    ("75492","Suraj Mishra01","Uttar Pradesh, Uttarakhand","Varanasi"),
    ("75291","Surendra Rawat","Gujarat","Surat"),
    ("75240","Suresh Balaji R","Tamil Nadu","Chennai"),
    ("75498","Surya Prakash","Uttar Pradesh, Uttarakhand","Lucknow"),
    ("75874","Surya Pratap Singh","Rajasthan","Jodhpur"),
    ("75100","Sushant Koul","Gujarat","Vadodara"),
    ("75758","Suyash Srivastava","Uttar Pradesh, Uttarakhand","Varanasi"),
    ("75479","Swagata Roy","Rest of Bengal, North East","South Bengal"),
    ("75092","Swapnil Dambale","Rest of Maharashtra, Goa","Aurangabad"),
    ("75443","TARUN RAWAL","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Haryana"),
    ("75490","Tanay Debnath","Rest of Bengal, North East","North East Cluster"),
    ("75761","Tanishca Gupta","Uttar Pradesh, Uttarakhand","Lucknow"),
    ("75175","Tarun Aggarwal","Uttar Pradesh, Uttarakhand","Moradabad"),
    ("75437","Tarun Kumar","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jalandhar"),
    ("75774","Utkarsh Nautiyal","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jalandhar"),
    ("75388","Vaibhav Kunj Sanjanwala","Gujarat","Surat"),
    ("75795","Vaibhavi Kumari","Bihar, Jharkhand, Orissa, Chattisgarh","Patna"),
    ("75856","Vaishnavi Angadi","Karnataka","Bangalore-1"),
    ("75703","Vasanthula Mohana Rao","Andhra Pradesh, Telangana","Vishakhapatnam"),
    ("75571","Veerla Vijaya Raju","Andhra Pradesh, Telangana","Hyderabad"),
    ("75681","Venkatesan Murali","Tamil Nadu","Coimbatore"),
    ("75787","Venkteshwar Tiwari","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75476","Vijayarajan B","Tamil Nadu","Trichy"),
    ("75669","Vikas Kumar","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
    ("75434","Vinay Sharma","Uttar Pradesh, Uttarakhand","Dehradun"),
    ("75469","Vinod Kumar Karra","Andhra Pradesh, Telangana","Hyderabad"),
    ("75812","Viraj Vasudev Naik","Rest of Maharashtra, Goa","Goa"),
    ("75807","Virendra Soni","Madhya Pradesh","Indore"),
    ("75565","Vishal Anil Awad","Rest of Maharashtra, Goa","Nagpur"),
    ("75718","Vishal Bhattacharjee","Rest of Bengal, North East","North East Cluster"),
    ("75247","Vishal Kela","Rest of Maharashtra, Goa","Pune"),
    ("75163","Vishal Tiwari","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jalandhar"),
    ("75708","Vivek Dhiman","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Jalandhar"),
    ("75722","Yajnesha Shettigar","Karnataka","South Karnataka"),
    ("75751","Yashasvi Jain","Delhi, NCR","NCR-MFD"),
    ("75706","Yashwant Moger","Karnataka","South Karnataka"),
    ("75809","Yogesh Bediskar","Gujarat","Surat"),
    ("75390","Yogesh Bhalgama","Gujarat","Ahmedabad"),
    ("75547","Yogesh Sakpal","","Virtual"),
    ("00116","Ashish Thakur","Punjab, Haryana, Himachal Pradesh, Jammu Kashmir","Chandigarh"),
    ("00117","Prashant Dhanawade","Delhi, NCR","NCR"),
    ("00068","Somesh Nivalkar","Mumbai","Mumbai"),
    ("00137","Muskan Kumari","Uttar Pradesh, Uttarakhand","Lucknow"),
    ("00136","Avinash Kamanaboina","Rest of Maharashtra, Goa","Pune"),
    ("00083","Satyaprakash Mishra","Bihar, Jharkhand, Orissa, Chattisgarh","Rest Of Bihar"),
    ("00142","Spandan Sabat","Bihar, Jharkhand, Orissa, Chattisgarh","Orissa"),
    ("00125","Karthik K","Kerala","Kerala"),
    ("00105","Uttam Kumar","Bihar, Jharkhand, Orissa, Chattisgarh","Jharkhand"),
]

# Create users
created = skipped = 0

def make_user(ec, name, role, pw, region=None, cluster=None, has_bic=False, bic_ec=None):
    global created, skipped
    if db.query(User).filter(User.emp_code==ec).first():
        skipped += 1; return
    db.add(User(emp_code=ec, name=name, role=role, region=region or None,
                cluster=cluster or None, has_bic_data=has_bic, bic_emp_code=bic_ec,
                hashed_password=hp(pw), is_active=True,
                must_change_pw=(role!="COE"), created_by="SYSTEM"))
    created += 1

for ec, name, role, pw in COE_USERS:
    make_user(ec, name, role, pw)

for ec, name, role, pw, region in RBH_USERS:
    make_user(ec, name, role, pw, region)

for row in CBH_USERS:
    ec, name, bic_ec, has_bic, region, cluster = row
    make_user(ec, name, "CBH", "Welcome@123", region, cluster, has_bic, bic_ec if has_bic else None)

for ec, name, region, cluster in BIC_USERS:
    # Skip if already created as CBH
    if db.query(User).filter(User.emp_code==ec).first():
        skipped += 1; continue
    make_user(ec, name, "BIC", "Welcome@123", region, cluster)

db.commit()
print(f"  ✓ {created} accounts created, {skipped} already existed")
print(f"  Total: {3} COE + {15} RBH + {58} CBH + {261} BIC = 337 accounts")

db.close()

# ── Step 5 (was 6): Summary ───────────────────────────
print(f"""
[5/5] Setup complete!
══════════════════════════════════════════════════════
  Database : MySQL (bfam_sales)
  Users    : 337 accounts (3 COE · 15 RBH · 58 CBH · 261 BIC)

Login credentials:
┌────────────┬──────────────────┬──────┬─────────────┐
│ Emp Code   │ Name             │ Role │ Password    │
├────────────┼──────────────────┼──────┼─────────────┤
│ ADMIN01    │ COE Admin 1      │ COE  │ Admin@123   │
│ ADMIN02    │ COE Admin 2      │ COE  │ Admin@123   │
│ ADMIN03    │ COE Admin 3      │ COE  │ Admin@123   │
│ RBH01      │ RBH - AP, Tel.  │ RBH  │ Welcome@123 │
│ CBH03      │ Anantharaman S  │ CBH  │ Welcome@123 │
│ 75189      │ Abhishek Pal    │ BIC  │ Welcome@123 │
│ 75612      │ Aman Verma      │ BIC  │ Welcome@123 │
└────────────┴──────────────────┴──────┴─────────────┘
  Note: All non-admin users must change password on first login.

Next steps:
  1. Start the backend:
       uvicorn app:app --host 0.0.0.0 --port 8000 --reload

  2. Open a second terminal and serve the frontend:
       python -m http.server 3000

  3. Open Chrome and go to:
       http://localhost:3000/bfam_platform.html

  4. Log in as ADMIN01 / Admin@123

  5. Go to Data Upload in the sidebar and upload your Excel files.
     The system will auto-identify each file and load the data.
""")
