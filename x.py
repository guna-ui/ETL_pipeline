# for results in data:
#                     transaction_id=results['transaction_id']
#                     customer_id   =results['customer_id']
#                     product_name  =results['product_name']
#                     category      =results['category']   
#                     quantity      =results['quantity']
#                     unit_price    =results['unit_price']
#                     tax_rate      =results['tax_rate']
#                     total_amount  =results['total_amount']
#                     txn_date      =results['transaction_date']
#                     location      =results['store_location']
#                     self.data.append(transaction_id,customer_id,product_name,category,quantity,unit_price,tax_rate,total_amount,txn_date,location)

#                     # f.executemany("insert into sales(ID,CUSTOMERID,PRODNAME,CATEGORY,QUANTITY,UNITPRICE,TOTALAMT,TXN_DATE,LOCATION)" \
#                 # "values(?,?,?,?,?,?,?,?,?)",self.data)        
#                 # print(f"insert the records of the count:{cursor.rowcount}")


x={'id': 'T1001', 'customer_id': 'C5502', 'product_name': 'Wireless Mouse', 'category': 'Electronics', 'quantity': 2, 'unit_price': 25.0, 'original_total': 54.0, 'calculated_total': 54.0}
print(x.keys())