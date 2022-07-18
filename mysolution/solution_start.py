import os
import json
import pandas as pd

#This Function is to get the folder name of each json file in transactions folder e.g d=2018-12-01
# this will return a nested list divided weekly 
# func will return list [["d=2018-12-01","d=2018-12-02","d=2018-12-03","d=2018-12-04","d=2018-12-05",
# "d=2018-12-06","d=2018-12-01"],[...]]
def getfile_path(filepath):
    all_files=[]  #intializing to append all week list inside it
    cnt=0 #intializing a count to get only 7 records 
    week=[] #intializing a list to appedn a particular week file name
    for files in os.listdir(filepath):
        if cnt<7:
            week.append(files)
            cnt+=1
        else:
            all_files.append(week)
            week=[]
            week.append(files)
            cnt=1
    return all_files
# This function will create a dataframe for a single week since it has two arguments r which is an nested array we are getting from above function and index which is a week no.

def makeDataFrame_by_week(r,indx):
    for i in range(len(r[indx])):
        if i==0: #for very jason file inside a particular week if there no dataframe created it will create 
            data=pd.read_json('../input_data/starter/transactions/'+r[indx][i]+'/transactions.json',lines=True)
        else: # Else it will append data inside it
            data2=pd.read_json('../input_data/starter/transactions/'+r[indx][i]+'/transactions.json',lines=True)
            data=data.append(data2)
    data["purchase_count"]=data["basket"].apply(len) #this is to add the purchase count column by calculating no. of orders inside a basket which len(basket)
    return data
# This Data frame will have customer_id,basket(which is a dic column containg prducts),date f purchase,purchase_count
# Now we have a week wise datafram we will have only those columns which we require and return a ew dataframe
def per_processing(df):
    new=pd.DataFrame(columns = ['customer_id', 'product_id', "purchase_count"])
    cnt=0
    for i,r in df.iterrows():
        for j in r[1]:
            p=[r[0],j["product_id"],r[3]]
            new.loc[cnt]=p
            cnt+=1
    return new
# this data frame will have customer_id,product_id (this we get from the basket dictonary),purchase_count
    


# The main function will make all the dataframe weekly merge it with customer and prodct data and convert the resultant dataset into JSON and save it inside the desired folder

def main():
    weeklylist=getfile_path('../input_data/starter/transactions/') #making a nested list of transcctions data folder splitted weekly
    for i in range(len(weeklylist)):
        path="../output_data/outputs/week"+str(i) # making a Dynammic string to store inside outputs_data folder
        df=makeDataFrame_by_week(weeklylist,i) #making data frame according to week index
        final=per_processing(df) #pre processing of data
        customerdf=pd.read_csv('../input_data/starter/customers.csv') #making customerdf from customer.csv file
        productdf=pd.read_csv('../input_data/starter/products.csv') #making product df from products.csv file
        final=pd.merge(final, customerdf, on ='customer_id', how ="inner") #inner joining the resultant df with customerdf on customer_id column
        final=pd.merge(final, productdf, on ='product_id', how ="inner") #inner joing the resultant df with productdf on product_id column
        final= final.drop('product_description',axis=1) # dropping not required columns from dataframe
        final.to_json(path_or_buf=path,orient='split',date_format='epoch',
                    double_precision = 2,force_ascii=False,date_unit='ms',default_handler=None,
                    lines=False,index=False,indent=2) #converting dataframe to json storing in our desired folder
    

if __name__ == "__main__":
    main()

