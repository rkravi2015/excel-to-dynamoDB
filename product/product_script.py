import xlrd
import boto3
import os
import sys 

REQUIRED_NO_OF_COLUMN = 4
ACCESS_KEY = os.getenv('DB_AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('DB_AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('DB_AWS_REGION')
TABLE_NAME = 'Product-table'
DYNAMODB_RESOURCE = 'dynamodb'
PRODUCT_NAME_TITLE = 'Product Name'
PRODUCT_ID_TITLE = 'Product ID'
DESCRIPTION_TITLE = 'Description'
PRICE_TITLE = 'Price'


# is_valid_row checks if row is a valid data row
def is_valid_row(row):
	if len(row)< REQUIRED_NO_OF_COLUMN:
		return False
	if '' in row:
		return False
	return True	


# get_row_with_title returns map of title and data for each row	
def get_row_with_title(title, row):
	row_with_title = {}
	for index in range(len(row)):
		row_with_title[title[index]] = str(row[index])
	return row_with_title	


# read_sheet returns list of data item in excel
def read_sheet(sheet):
	row_values=[]
	title=[]
	for index in range(sheet.nrows):
		row = sheet.row_values(index)
		if is_valid_row(row):
			if len(title)==0:
				title = row
				continue
			single_row = get_row_with_title(title, row)
			row_values.append(single_row)
	return row_values	


# put_item_to_SLIN uploads data to dynamodb
def put_item_to_product_table(session, data):
	dynamodb = session.resource(DYNAMODB_RESOURCE)
	table = dynamodb.Table(TABLE_NAME)
	for item in data:
		add_item={
		'pkey': item[PRODUCT_ID_TITLE],
		'product_id': item[PRODUCT_ID_TITLE],
		'product_name': item[PRODUCT_NAME_TITLE],
		'description': item[DESCRIPTION_TITLE],
		'price': item[PRICE_TITLE]}
		response = table.put_item(
			Item=add_item					
			)
		print("response status code for SLIN: ", item[PRODUCT_ID_TITLE], " is: ", response['ResponseMetadata']['HTTPStatusCode']) 	


if __name__ == "__main__":
	file_loc = sys.argv[1]
	work_book = xlrd.open_workbook(file_loc)
	sheet_names = work_book.sheet_names()
	session = boto3.Session(
			aws_access_key_id=ACCESS_KEY,
			aws_secret_access_key=SECRET_KEY,
			region_name=REGION)
	for sheet in range(len(sheet_names)):
		values = read_sheet(work_book.sheet_by_index(sheet))
		put_item_to_product_table(session, values)
		

        