# F1 Data Fetcher for Databricks

Fetch and store historical Formula 1 telemetry data using the OpenF1 API. Designed for use in Databricks with MongoDB storage.

---

## 🔧 Setup (Databricks)

1. Clone the repository to your local machine:

bash
git clone https://github.com/KakoulisM/f1-data-fetcher-databricks.git

2. Create a Databricks Secret Scope and add:

        openf1_api_key

        mongo_uri

3. Upload and open notebooks/fetch_f1_data_notebook.py in Databricks.

4. Install Python dependencies in a cell:

%pip install requests pandas numpy pymongo

MongoDB Atlas Setup (Step-by-Step)
1. Create MongoDB Atlas Account

    Go to: https://www.mongodb.com/cloud/atlas

    Create a free account (choose Shared Cluster, it's free).

    Choose a cloud provider and region closest to your Databricks workspace (e.g., AWS Europe West for EU users).

2. Create a Cluster

    Click "Create a Cluster" → Choose free M0.

    Wait a few minutes for deployment.

3. Create a Database and Collection

    Go to "Database" > Browse Collections.

    Click "Create Database".

    Set:

        Database name: f1data

        Collection name: realtime

    Click "Create".

4. Create a Database User

    Go to "Database Access" > "+ Add New Database User".

    Username: kakoulisminas

    Password: 6984770434Ser! (as per your script — or change it for security).

    Database User Privileges: Leave as Read and Write to Any Database.

    Click "Add User".

5. Allow Access from Anywhere

    Go to "Network Access" → Click "Add IP Address".

    Select "Allow Access from Anywhere" (0.0.0.0/0).

    Or, if you know the Databricks IP, whitelist that instead.

6. Get the MongoDB Connection URI

    Go to "Database" > Connect > Drivers.

    Choose Python and Version 3.12 or higher.

    You’ll see a URI like this:

mongodb+srv://<username>:<password>@<cluster-name>.mongodb.net/?retryWrites=true&w=majority&appName=<app
edit username and password and copy in vault where mongo uri was made

RUN CODE
