import paramiko
import datetime
import re
import csv
import json
import pyodbc
import requests
import traceback
import keyring
from lxml import etree

# SFTP Configuration
SFTP_HOST = "files.ftphosting.com"
SFTP_PORT = 22
SFTP_USERNAME = "Acosta"
SFTP_PASSWORD = "*Sb4smZrAgv"
SFTP_REMOTE_DIR = "./"
SFTP_PROCESSED_DIR = "Processed/"

# VeraCore API Configuration
VERACORE_API_URL = "https://rhu351.veracore.com/pmomsws/oms.asmx"

# Function to log errors to the database
def log_error_to_db(notification_type, error_message, file_name = ""):
    try:
        conn = pyodbc.connect(
            "DRIVER={SQL Server Native Client 11.0};"
            "SERVER=66.185.24.59;"
            "DATABASE=VeraCoreOrderProcessing;"
            "UID=AisleLogicSQL;"
            "PWD=3VnZZVXQRbvU4Qv;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
        cursor = conn.cursor()
        query = "INSERT INTO AutomatedProcessLogs (ProcessName, NotificationType, FileName, ErrorMessage, ErrorDateTime) VALUES ('GortonsAcostaOrderUpload', ?, ?, ?, ?)"
        cursor.execute(query, (notification_type, file_name, error_message, datetime.datetime.now()))
        conn.commit()
        conn.close()
    except Exception as db_error:
        print(f"Failed to log error to database: {db_error}")

# Function to get VeraCore API token
def get_api_token(system_id):
    try:
        credentials_json = keyring.get_password("veracore_api", system_id)
        if credentials_json:
            token = json.loads(credentials_json)
            apitoken = token.get("APIToken")
            return apitoken
    except Exception as e:
        error_details = f"{str(e)}\n{traceback.format_exc()}"  # Includes stack trace
        log_error_to_db("Error", f"Error fetching API token for SystemID {system_id}: {e}")
        #print(f"Error fetching API token for SystemID {system_id}: {e}")
    return None

# Function to generate VeraCore report
def generate_veracore_report(api_token, report_name):
    try:
        url = "https://rhu351.veracore.com/veracore/public.api/api/reports"
        headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
        payload = {"reportName": report_name}
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        error_details = f"{str(e)}\n{traceback.format_exc()}"  # Includes stack trace
        log_error_to_db("Error", f"Error generating VeraCore report: {e}")
        #print(f"Error generating VeraCore report: {e}")
    return None

# Function to fetch report data
def fetch_report_data(task_id, api_token):
    try:
        url = f"https://rhu351.veracore.com/veracore/public.api/api/reports/{task_id}"
        headers = {"Authorization": f"Bearer {api_token}"}
        response = requests.get(url, headers=headers)
        while response.status_code != 200:
            response = requests.get(url, headers=headers)
        return response.json()
    except Exception as e:
        error_details = f"{str(e)}\n{traceback.format_exc()}"  # Includes stack trace
        log_error_to_db("Error", f"Error fetching report data for TaskID {task_id}: {e}")
        #print(f"Error fetching report data for TaskID {task_id}: {e}")
    return None

# Function to extract Product IDs from the VeraCore report
def extract_valid_skus_from_report(report_data):
    # Assuming the 'Product ID' is stored within each item in the report
    valid_skus = set()  # Using a set for faster lookups
    for item in report_data["Data"]:
        product_id = item.get("Product ID")  # Modify according to the actual structure of the report
        if product_id:
            valid_skus.add(product_id)
    return valid_skus

# Function to find the latest orders CSV file
def get_latest_orders_file():
    with paramiko.Transport((SFTP_HOST, SFTP_PORT)) as transport:
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            files = sftp.listdir(SFTP_REMOTE_DIR)
            
            # Regex to match the file naming pattern
            pattern = re.compile(r"OrdersToSend_\d{4}-\d{2}-\d{2}_\d{6}\.csv")

            # Find the matching file
            matching_files = [f for f in files if pattern.match(f)]
            if not matching_files:
                raise FileNotFoundError("No orders file found.")

            latest_file = matching_files[0]  # Since there's only 1 file
            #print(f"Latest file found: {latest_file}")
            return latest_file

# Function to fetch and read CSV from SFTP
def fetch_csv_from_sftp():
    latest_file = get_latest_orders_file()
    remote_file_path = f"{SFTP_REMOTE_DIR}{latest_file}"

    with paramiko.Transport((SFTP_HOST, SFTP_PORT)) as transport:
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            with sftp.file(remote_file_path, mode="r") as remote_file:
                csv_data = remote_file.read().decode("utf-8-sig")  # Use utf-8-sig to remove BOM
    
    return csv_data, latest_file

# Function to validate CSV data
def validate_csv_data(csv_content, valid_skus, filename):
    orders = {}
    reader = csv.DictReader(csv_content.splitlines())

    required_fields = ["Order ID", "Order Date", "Order By Company", "Order By First Name", "Order By Last Name", "Order By Address 1", "Order By City", "Order By State", "Order By Zip", "Order By Country", "SKU ID", "Qty",  "Shipping Option", "Commercial",
                        "Ship To Company", "Ship To First Name", "Ship To Last Name", "Ship To Address 1", "Ship To City", "Ship To State", "Ship To Zip", "Ship To Country", "OrderOwner"] # Must be non-empty
    optional_fields = ["Order By Address 2", "Order By Phone", "Ship Comments", "Ship To Address 2", "Ship To Phone"]  # Can be empty

    # Zip code validation regex (5 or 9 digits)
    zip_pattern = re.compile(r"^\d{5}(-\d{4})?$")
    # Order ID validation regex (starts with "Acosta" followed by digits)
    order_id_pattern = re.compile(r"^Acosta\d+$")

    for row in reader:
        missing_fields = [field for field in required_fields if field not in row or not row[field].strip()]

        if missing_fields:
            log_error_to_db("Error", f"Invalid row found: {row} \nMissing or empty fields: {', '.join(missing_fields)}", filename)
            #print(f"Invalid row found: {row}")
            #print(f"Missing or empty fields: {', '.join(missing_fields)}")
            continue  # Skip invalid rows

        # Validate Order Date (check if it's a valid datetime)
        try:
            order_date = datetime.datetime.strptime(row["Order Date"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            log_error_to_db("Error", f"Invalid Order Date in row: {row}", filename)
            #print(f"Invalid Order Date in row: {row}")
            continue  # Skip this row if Order Date is invalid

        # Validate Order ID (starts with Acosta and followed by numbers, and no underscores)
        if not order_id_pattern.match(row["Order ID"]) or "_" in row["Order ID"]:
            log_error_to_db("Error", f"Invalid Order ID in row: {row} \n Order ID must start with 'Acosta' followed by a number and should not contain underscores. Found: {row['Order ID']}", filename)
            #print(f"Invalid Order ID in row: {row}")
            #print(f"Order ID must start with 'Acosta' followed by a number and should not contain underscores. Found: {row['Order ID']}")
            continue  # Skip this row if Order ID is invalid

        # Validate Shipping Option (must be 92)
        if row["Shipping Option"].strip() != "92":
            log_error_to_db("Error", f"Invalid Shipping Option in row: {row}", filename)
            #print(f"Invalid Shipping Option in row: {row}")
            continue  # Skip this row if Shipping Option is not 92

        # Validate Commercial (must be 1)
        if row["Commercial"].strip() != "1":
            log_error_to_db("Error", f"Invalid Commercial value in row: {row}", filename)
            #print(f"Invalid Commercial value in row: {row}")
            continue  # Skip this row if Commercial is not 1

        # Validate SKU ID (must exist in the set of valid SKUs from VeraCore)
        if row["SKU ID"].lower() not in [sku.lower() for sku in valid_skus]:
            log_error_to_db("Error", f"Invalid SKU ID in row: {row}", filename)
            #print(f"Invalid SKU ID in row: {row}")
            continue  # Skip this row if SKU ID is not valid

        # Validate Zip codes
        for zip_field in ["Order By Zip", "Ship To Zip"]:
            zip_code = row.get(zip_field, "").strip()
            if zip_code and not zip_pattern.match(zip_code):
                log_error_to_db("Error", f"Invalid zip code in row: {row} \n Field '{zip_field}' has an invalid value: {zip_code}", filename)
                #print(f"Invalid zip code in row: {row}")
                #print(f"Field '{zip_field}' has an invalid value: {zip_code}")
                continue  # Skip this row if zip code is invalid

        # Accumulate the item for the given order
        order_id = row["Order ID"]
        item = {
            "product_id": row["SKU ID"],
            "quantity": int(row["Qty"])
        }

        if order_id not in orders:
            # Create a new order entry if it doesn't exist
            orders[order_id] = {
                "order_id": order_id,
                "order_date": order_date.isoformat(),
                "order_by_company": row['Order By Company'],
                "order_by_first_name": row['Order By First Name'],
                "order_by_last_name": row['Order By Last Name'],
                "order_by_address1": row["Order By Address 1"],
                "order_by_address2": row["Order By Address 2"],
                "order_by_city": row['Order By City'],
                "order_by_state": row['Order By State'],
                "order_by_zip_code": row['Order By Zip'],
                "order_by_country": row['Order By Country'],
                "order_by_phone": row['Order By Phone'],

                "ship_to_company": row['Ship To Company'],
                "ship_to_first_name": row['Ship To First Name'],
                "ship_to_last_name": row['Ship To Last Name'],
                "ship_to_address1": row['Ship To Address 1'],
                "ship_to_address2": row['Ship To Address 2'],
                "ship_to_city": row['Ship To City'],
                "ship_to_state": row['Ship To State'],
                "ship_to_zip_code": row['Ship To Zip'],
                "ship_to_country": row['Ship To Country'],
                "ship_to_phone": row['Ship To Phone'],
                "ship_to_commercial": row['Commercial'],
                "ship_to_comments": row['Ship Comments'],
                "shipping_option": row['Shipping Option'],
                "stream": row['OrderOwner'],

                "items": [item]  # Start with the first item
            }
        else:
            # If the order already exists, just append the new item
            orders[order_id]["items"].append(item)

    # Convert the order dictionary to a list of orders for final output
    orders_list = list(orders.values())

    if not orders_list:
        raise ValueError("No valid orders found in the CSV.")

    return orders_list

# Function to move processed file to 'Processed' folder
def move_file_to_processed(filename):
    source_path = f"{SFTP_REMOTE_DIR}{filename}"
    destination_path = f"{SFTP_PROCESSED_DIR}{filename}"

    with paramiko.Transport((SFTP_HOST, SFTP_PORT)) as transport:
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.rename(source_path, destination_path)
            #print(f"Moved file to: {destination_path}")
    return

# Function to generate SOAP XML request
def create_soap_request(order_data):
    nsmap = {
    "soap": "http://schemas.xmlsoap.org/soap/envelope/",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "xsd": "http://www.w3.org/2001/XMLSchema"
    }

    # Create the Envelope element with the correct namespace handling
    envelope = etree.Element("{http://schemas.xmlsoap.org/soap/envelope/}Envelope", nsmap=nsmap)

    #envelope = etree.Element("soap:Envelope", nsmap={"soap": "http://schemas.xmlsoap.org/soap/envelope/"})
    header = etree.SubElement(envelope, "{http://schemas.xmlsoap.org/soap/envelope/}Header")

    # Header Information
    auth = etree.SubElement(header, "AuthenticationHeader", xmlns="http://omscom/")
    etree.SubElement(auth, "Username").text = "Acosta" #VSO351
    etree.SubElement(auth, "Password").text = "QBuPCs3&uo"

    body = etree.SubElement(envelope, "{http://schemas.xmlsoap.org/soap/envelope/}Body")
    add_order = etree.SubElement(body, "AddOrder", xmlns="http://omscom/")

    # Order Information
    order = etree.SubElement(add_order, "order")

    # Order Header Information
    header = etree.SubElement(order, "Header")

    etree.SubElement(header, "ID").text = order_data["order_id"]
    etree.SubElement(header, "EntryDate").text = order_data["order_date"]
    etree.SubElement(header, "Stream").text = "Acosta 360"
    etree.SubElement(header, "Comments").text = "Acosta 360"
    etree.SubElement(header, "View").text = "Default"

    # Order Variables Information
    orderVariables = etree.SubElement(order, "OrderVariables")
    orderVariable = etree.SubElement(orderVariables, "OrderVariable")
    variableField = etree.SubElement(orderVariable, "VariableField")
    etree.SubElement(variableField, "FieldName").text = "OrderType"
    etree.SubElement(orderVariable, "Value").text = "True"
    etree.SubElement(orderVariable, "ValueDescription").text = "Acosta 360 Import"

    orderVariable2 = etree.SubElement(orderVariables, "OrderVariable")
    variableField2 = etree.SubElement(orderVariable2, "VariableField")
    etree.SubElement(variableField2, "FieldName").text = "OrderOwner"
    etree.SubElement(orderVariable2, "Value").text = "True"
    etree.SubElement(orderVariable2, "ValueDescription").text = "Acosta 360"

    # Order By Information
    orderedby = etree.SubElement(order, "OrderedBy")
    
    etree.SubElement(orderedby, "CompanyName").text = order_data["order_by_company"]
    etree.SubElement(orderedby, "FirstName").text = order_data["order_by_first_name"]
    etree.SubElement(orderedby, "LastName").text = order_data["order_by_last_name"]
    etree.SubElement(orderedby, "Address1").text = order_data["order_by_address1"]
    etree.SubElement(orderedby, "Address2").text = order_data["order_by_address2"]
    etree.SubElement(orderedby, "City").text = order_data["order_by_city"]
    etree.SubElement(orderedby, "State").text = order_data["order_by_state"]
    etree.SubElement(orderedby, "PostalCode").text = order_data["order_by_zip_code"]
    etree.SubElement(orderedby, "Country").text = order_data["order_by_country"]
    etree.SubElement(orderedby, "Phone").text = order_data["order_by_phone"]
    etree.SubElement(orderedby, "TaxExempt").text = "false"
    etree.SubElement(orderedby, "TaxExemptApproved").text = "false"
    etree.SubElement(orderedby, "Commercial").text = "true"

    # Ship To Information    
    shipto = etree.SubElement(order, "ShipTo")

    shiptoorderby = etree.SubElement(shipto, "OrderShipTo")
    etree.SubElement(shiptoorderby, "Key").text = "0"
    etree.SubElement(shiptoorderby, "CompanyName").text = order_data["ship_to_company"]
    etree.SubElement(shiptoorderby, "FirstName").text = order_data["ship_to_first_name"]
    etree.SubElement(shiptoorderby, "LastName").text = order_data["ship_to_last_name"]
    etree.SubElement(shiptoorderby, "Address1").text = order_data["ship_to_address1"]
    etree.SubElement(shiptoorderby, "Address2").text = order_data["ship_to_address2"]
    etree.SubElement(shiptoorderby, "City").text = order_data["ship_to_city"]
    etree.SubElement(shiptoorderby, "State").text = order_data["ship_to_state"]
    etree.SubElement(shiptoorderby, "PostalCode").text = order_data["ship_to_zip_code"]
    etree.SubElement(shiptoorderby, "Country").text = order_data["ship_to_country"]
    etree.SubElement(shiptoorderby, "Phone").text = order_data["ship_to_phone"]
    etree.SubElement(shiptoorderby, "Commercial").text = order_data["ship_to_commercial"]
    etree.SubElement(shiptoorderby, "Comments").text = order_data["ship_to_comments"]
    specialHandling = etree.SubElement(shiptoorderby, "SpecialHandling")
    etree.SubElement(specialHandling, "Description").text = "FedEx Ground"

    # Bill To Information
    billto = etree.SubElement(order, "BillTo")

    etree.SubElement(billto, "Flag").text = "OrderedBy"

    # Offers Information
    items = etree.SubElement(order, "Offers")
    for item in order_data["items"]:
        offer_ordered = etree.SubElement(items, "OfferOrdered")
        offer = etree.SubElement(offer_ordered, "Offer")
        header = etree.SubElement(offer, "Header")
        etree.SubElement(header, "ID").text = item["product_id"]
        etree.SubElement(offer_ordered, "Quantity").text = str(item["quantity"])
        ordershipto = etree.SubElement(offer_ordered, "OrderShipTo")
        etree.SubElement(ordershipto, "Key").text = "0"
    
    #print(etree.tostring(envelope, pretty_print=True, encoding="utf-8", xml_declaration=True))
    return etree.tostring(envelope, pretty_print=True, encoding="utf-8", xml_declaration=True)

# Function to send SOAP request
def send_soap_request(soap_xml):
    headers = {"Content-Type": "text/xml; charset=utf-8"}
    response = requests.post(VERACORE_API_URL, data=soap_xml, headers=headers)
    if response.status_code != 200:
        error_message = response.text
        log_error_to_db("Error", f"SOAP request failed with status code {str(response.status_code)}: {error_message}", filename)
    return response.text

# Function to process orders and submit them
def process_orders_and_submit(orders):
    for order_data in orders: # No field order_data, not sure if we need to define every field?
        soap_request = create_soap_request(order_data)
        #print(soap_request)
        #send_soap_request(soap_request)
        response = send_soap_request(soap_request)

        #print(f"Response for Order {order_id}: {response}")
    return response

# Main Process
if __name__ == "__main__":
    try:
        log_error_to_db("Notification", "Process Started.")
        #print("Fetching CSV from SFTP...")
        csv_content, filename = fetch_csv_from_sftp()

        report_name = "Acosta SKUs"
        api_token = get_api_token("AL0001")
        
        if api_token:
            report_data = generate_veracore_report(api_token, report_name)
            
            if report_data:
                task_id = report_data.get("TaskId")
                report_data = fetch_report_data(task_id, api_token)
                
                if report_data:
                    #print("Report Data Type:", type(report_data))
                    #print("Report Data Content:", json.dumps(report_data, indent=2))

                    #print("Report data fetched successfully.")
                    valid_skus = extract_valid_skus_from_report(report_data)

        #print("Validating CSV data...")
        orders = validate_csv_data(csv_content, valid_skus, filename)

        #print("Processing orders and creating JSON payload...")
        process_orders_and_submit(orders)
        
        #print("Moving processed file to 'Processed' folder...")
        move_file_to_processed(filename)
        
        log_error_to_db("Notification", "Process Completed.")
        #print("Process completed successfully.")
    
    except FileNotFoundError as e:
        log_error_to_db("Notification", "No file found.")
        log_error_to_db("Notification", "Process Completed.")
    
    except ValueError as e:
        error_details = f"{str(e)}\n{traceback.format_exc()}"
        log_error_to_db("Error", "No valid orders found in the CSV.")
        log_error_to_db("Notification", "Process Completed.")

    except Exception as e:
        error_details = f"{str(e)}\n{traceback.format_exc()}"  # Includes stack trace
        log_error_to_db("Error", error_details)
        log_error_to_db("Notification", "Process Completed.")
        #print(f"Error: {e}")
