import csv
import logging

logging.basicConfig(
    level=logging.INFO,
    filename='app.log',
    format='%(levelname)s - %(message)s ',
    filemode='a'
)


class Basic_Pipeline:
    def __init__(self,file_path):
        self.file_path=file_path
        self.data=[]
    def read_csv(self):
        return "File not implemented error"
    def transform_data(self,data):
        self.data=data
        return "implementation should be done in subclasses"
class Read_file_class(Basic_Pipeline):
    def read_csv(self):
        self.data=[]
        try:
            logging.info(f"Started reading the file:{self.file_path} ")
            with open(self.file_path,mode='r',newline="")as f:
                reader=csv.DictReader(f)
                for row in reader:
                    self.data.append(row)
            return self.data
        except Exception as e:
            logging.error(f"Error while reading the file from path:{e}")

class Transform_csv(Basic_Pipeline):
    def transform_data(self,data):
        data_r=[]
        for row in data:
          try:
            quantity=int(row['quantity'])
            unit_price=float(row['unit_price'])
            tax_rate=float(row['tax_rate'])
            total_amount=float(row['total_amount'])
            calculated_total=quantity*unit_price*(1+tax_rate)
            data_r.append({
                    'id':row['transaction_id'],
                    'customer_id':row['customer_id'],
                    'product_name':row['product_name'],
                    'category':row['category'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'original_total': float(row['total_amount']),
                    'calculated_total': round(calculated_total, 2)
            })
          except (ValueError,KeyError) as e:
              logging.warning(f"Skipping row due to error :{e}")
        logging.info("Transformation successful")
        return data_r
        


            
        

read_file=Read_file_class('sales_data.csv')
data=read_file.read_csv()
transformer=Transform_csv('sales_data.csv')
final_data = transformer.transform_data(data)



   