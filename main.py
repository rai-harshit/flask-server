from flask import Flask, request #import main Flask class and request object
import sqlalchemy

app = Flask(__name__) #create the Flask app

@app.route("/")
def sayHello():
    return "Hello World!"

@app.route('/usage_data',methods=['POST'])
def postJasonHandler():
    # Parse the json data receieved from the client.
    json_data = request.get_json()
    # Identify the userId so as to verify if this user exists in the database.
    userId = json_data['userId']
    isVerifiedUser = verifyUser(userId)
    if isVerifiedUser:            
        print("[INFO] {} is a verified user.".format(userId))
        cleanedData = cleanReceivedData(json_data)
        sqlInsertStatus = insertDataToDB(cleanedData)
        if sqlInsertStatus:
            return "OK",200
    else:
        print("[INFO] {} is not a verified user. Discarding the data.".format(userId))
        return "User Not Found",404

def verifyUser(userId):
    return 1

def cleanReceivedData(json_data):
    processedDataList = []
    for rawString in json_data['payload']:
        processedDataList.append(rawString.strip().split(","))
    return processedDataList

def insertDataToDB(cleanedData):
    # Setting up config parameters
    connection_name = "qtech-mhs-dev:asia-south1:balanced-sql-playground"
    db_name = "balanced_db"
    db_user = "root"
    db_password = "Hr143@Eminem"
    driver_name = 'mysql+pymysql'
    query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})
    db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
        drivername=driver_name,
        username=db_user,
        password=db_password,
        database=db_name,
        query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )
    try:
        with db.connect() as conn:
            for record in cleanedData:
                stmt = "INSERT INTO balanced_user_data (userId,raw_app_name,raw_timestamp,raw_time_since_last_input,raw_audio_output)\
                     VALUES (%s,%s,%s,%s,%s)",(record[0],record[1],record[2],record[3],record[4])    
                print(stmt)
                conn.execute(stmt)
    except Exception as e:
        print("Insert failed with an exception: ",str(e))
        return -1
    return 1

if __name__ == '__main__':
    app.run(host="127.0.0.1",debug=True, port=8080) #run app in debug mode on port 5000