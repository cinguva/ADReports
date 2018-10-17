import credentials
import ast
from pyspark.sql import SparkSession
from ldap3 import Server, Connection, ALL, NTLM, SUBTREE
import json


def get_ldap_info(u):
    with Connection(Server('gogo.local', get_info=ALL),auto_bind=True,read_only=True,check_names=True,authentication=NTLM,user=credentials.ldap_user, password=credentials.ldap_password) as c:
        results = c.extend.standard.paged_search(search_base='ou=Accounts,dc=gogo,dc=local',search_filter='(&(objectClass=User)(|(employeeType=MIMWorker*)(employeeType=Employee*)(employeeType=ContingentWorker*)(employeeType=Intern*)))',search_scope=SUBTREE,attributes=['cn', 'manager', 'sAMAccountName', 'employeeType', 'extensionAttribute8'], get_operational_attributes=True)
    i = 0
    resObj=[]
    for item in results:
        resObj.append(ast.literal_eval(str(item['attributes'])))
        i += 1

    print(len(resObj))
    result_data= json.dumps(resObj)
    jsonObj=json.loads(result_data)
    with open('/Library/Tomcat/webapps/ROOT/UserExport/userexport.json', 'w') as outfile:
        json.dump(jsonObj,outfile)
    outfile.close()

def get_filtered_ldap_data(u):
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # Read the JSON raw data of a web service
    df = spark.read.json("/Library/Tomcat/webapps/ROOT/UserExport/userexport.json")

    # Filter data for all Termed users and save it to Dataframe
    df2 = df.filter(df['extensionAttribute8'].like('Terminated')).orderBy('cn', ascending=True)

    # Collect each row of data frame and append to a list
    users = {}
    users["items"] = []
    user_data = users["items"]
    data = df2.toJSON().collect()
    for row in data:
        user_data.append(ast.literal_eval(row))

    with open('/Library/Tomcat/webapps/ROOT/UserExport/termusers.json', 'w') as f:
        json.dump(user_data, f)
    f.close()
    spark.stop()


get_ldap_info('*')
get_filtered_ldap_data('*')

