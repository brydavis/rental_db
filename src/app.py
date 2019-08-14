import boto3
from boto3.dynamodb.conditions import Key, Attr

import csv
import json


# Get the service resource.
session = boto3.Session(profile_name="default")
dynamodb = session.resource('dynamodb', region_name='us-west-2')

def strip_non_ascii(string):
    ''' Returns the string without non ASCII characters'''
    stripped = (c for c in string if 0 < ord(c) < 127)
    return ''.join(stripped)

# Create the DynamoDB table.
def dynamo_create_table(table_name, key_schema, attribute_definitions):
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )      
        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)  
        print("table created")
        return True
    except Exception as e:
        print(e)
        return False
    

def dynamo_insert_one(table_name, item):
    table = dynamodb.Table(table_name)
    try:
        table.put_item(
            Item=item
         )
        return True
    except Exception as e:
        return False



def import_data(data_dir, *files):
    for filepath in files:
        collection_name = filepath.split(".")[0]
        
        print("opening", "/".join([data_dir, filepath]))
        with open("/".join([data_dir, filepath])) as file:
            reader = csv.reader(file, delimiter=",")
            
            header = False
            for row in reader:
                if not header:
                    header = [h.strip("\ufeff").strip("ï»¿").strip() for h in row]
                    dynamo_create_table(
                        collection_name,
                        [
                            {
                                'AttributeName': header[0],
                                'KeyType': 'HASH'
                            }
                        ],
                        [
                            {
                                'AttributeName': header[0],
                                'AttributeType': 'S'
                            },
                        ],
                    )
                else:
                    data = {header[i]:v for i,v in enumerate(row)}
                    print(data)
                    try:
                        dynamo_insert_one(collection_name, data)    
                    except Exception as e:
                        print(e)
                        print(data)
                        

"""pip install Flask"""                        
from flask import Flask, request, jsonify
app = Flask(__name__)

@app.route('/')
def index():
    with open("src/index.html") as file:
        return file.read()

@app.route('/all')
def all_data ():
    return json.dumps({
        "product": dynamodb.Table("product").scan()["Items"],
    })

@app.route('/select', methods=['POST'])
def select_data():
    if request.method == 'POST':  #this block is only entered when the form is submitted
        product_id = request.form["product_id"]
        rental = dynamodb.Table("rental")
        response = rental.scan(
            FilterExpression=Key('product_id').eq(product_id)
        )
        return json.dumps(response)
    else:
        return {}


if __name__ == "__main__":
    # import_data("data", "product.csv", "customer.csv", "rental.csv")
    app.run(port=8888)