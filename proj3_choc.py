import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

#create choc.db
def create_db():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # Drop Tables if need
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    conn.commit()

    # Create Tables
    statement = '''
        CREATE TABLE 'Bars' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Company' Text NOT NULL,
                'SpecificBeanBarName' TEXT NOT NULL,
                'REF' TEXT NOT NULL,
                'ReviewDate' TEXT NOT NULL,
                'CocoaPercent' REAL NOT NULL,
                'CompanyLocationId' INTEGER,
                'Rating' REAL NOT NULL,
                'BeanType' TEXT NOT NULL,
                'BroadBeanOriginId' INTEGER,
                FOREIGN KEY ('CompanyLocationId') REFERENCES Countries('Id'),                
                FOREIGN KEY ('BroadBeanOriginId') REFERENCES Countries('Id')                
        );        
    '''
    cur.execute(statement)


    statement = '''
        CREATE TABLE 'Countries' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Alpha2' TEXT NOT NULL,
                'Alpha3' TEXT NOT NULL,
                'EnglishName' TEXT NOT NULL,
                'Region' TEXT NOT NULL,
                'Subregion' TEXT,
                'Population' Integer,
                'Area' REAL
        );        
    '''
    cur.execute(statement)


    conn.commit()
    conn.close()    

def get_data():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    # populate json file
    with open(COUNTRIESJSON) as f:
        data = json.load(f)
    for i in data:
        id = i['numericCode']
        english_name = i['name']
        alpha2 = i['alpha2Code']
        alpha3 = i['alpha3Code']
        region = i['region']
        subregion = i['subregion']
        population = i['population']
        area = i['area']
        insertion = (id, alpha2, alpha3, english_name, region, subregion, population, area)
        statement = 'INSERT INTO "Countries" '
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)

    # populate csv file
    with open(BARSCSV) as bars:
        bar_data = list(csv.reader(bars))
    for bar in bar_data[1:]:
        #first find the country_id 
        company_location = bar[5]
        statement = '''
            SELECT Id FROM Countries
            WHERE EnglishName = ?
        '''
        cur.execute(statement, (company_location,))
        id_1 = None
        for i in cur:
            id_1 = i[0]
        bean_location = bar[8]
        statement = '''
            SELECT Id FROM Countries
            WHERE EnglishName = ?
        '''
        cur.execute(statement, (bean_location,))
        id_2 = None
        for i in cur:
            id_2 = i[0]

        # also change bar4 from percentage to decimal
        dec = float(bar[4].strip('%'))/100.0

        insertion = (None, bar[0], bar[1], bar[2], bar[3], dec, id_1, bar[6], bar[7], id_2)
        statement = 'INSERT INTO "Bars" '
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)
    conn.commit()
    conn.close()


# Part 2: Implement logic to process user commands

def get_num_or_name(command, word):
    if command.find(word) != -1:
        index = command.find(word)
        index += len(word)
        try:
            num = command[index:].split(' ')[0]
        except:
            num = None
        return num


def process_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # commend for bars
    if command[:4] == 'bars':
        ste_tmp = ''
        values = ()
        if command.find('sellcountry=') != -1:
            code = get_num_or_name(command, 'sellcountry=')
            ste_tmp += 'WHERE co_1.Alpha2 = ? '
            values += (code,)
        elif command.find('sourcecountry=') != -1:
            code = get_num_or_name(command, 'sourcecountry=')
            ste_tmp += 'WHERE co_2.Alpha2 = ? '
            values += (code,)
        elif command.find('sellregion=') != -1:
            name = get_num_or_name(command, 'sellregion=')
            ste_tmp += 'WHERE co_1.Region = ? '
            values += (name,)
        elif command.find('sourceregion=') != -1:
            name = get_num_or_name(command, 'sourceregion=')
            ste_tmp += 'WHERE co_2.Region = ? '
            values += (name,)

        if command.find('cocoa') != -1:
            ste_tmp += ' ORDER BY CocoaPercent '
        else:
            ste_tmp += ' ORDER BY Rating '

        if command.find('bottom=') != -1:
            num = get_num_or_name(command, 'bottom=')
            ste_tmp += ' ASC LIMIT ?'
            values += (num,)
        else:
            num = 10
            if command.find('top=') != -1:
                num = get_num_or_name(command, 'top=')
            ste_tmp += 'DESC LIMIT ? '
            values += (num,)

        sub_command = command[4:].strip(' ')
        if sub_command != '':
            if sub_command.find('sellcountry=') == -1 and sub_command.find('sourcecountry=') == -1 and sub_command.find('sellregion=') == -1 and sub_command.find('sourcecountry=') == -1 and sub_command.find('ratings') == -1 and sub_command.find('cocoa') == -1 and sub_command.find('top=') == -1 and sub_command.find('bottom=') == -1:
                return -1

        # write statement for bars
        statement = '''
            SELECT ba.SpecificBeanBarName, ba.Company, co_1.EnglishName, ba.Rating, ba.CocoaPercent, co_2.EnglishName FROM Bars AS ba
            LEFT JOIN Countries AS co_1
            ON co_1.Id = ba.CompanyLocationId
            LEFT JOIN Countries AS co_2
            ON co_2.Id = ba.BroadBeanOriginId '''
        statement += ste_tmp
        # print(statement)
        # print(values)
        cur.execute(statement, values)
        re = []
        for i in cur:
            m = ()
            for j in range(len(i)):
                if j == 4:
                    k = (str(i[j] * 100) + '%')
                    m += (k,)
                else:
                    m += (i[j],)

            re.append(m)  

    if command[:9] == 'companies':
        ste_tmp = ''
        values = ()

        if command.find('country=') != -1:
            code = get_num_or_name(command, 'country=')
            ste_tmp += 'WHERE co_1.Alpha2 = ? '
            values += (code,)
        elif command.find('region=') != -1:
            name = get_num_or_name(command, 'region=')
            ste_tmp += 'WHERE co_1.Region = ? '
            values += (name,)

        ste_tmp += " GROUP BY ba.Company HAVING COUNT(ba.Id) > 4"

        if command.find('cocoa') != -1:
            ste_tmp += ' ORDER BY AVG(ba.CocoaPercent) '
        elif command.find('bars_sold') != -1:
            ste_tmp += ' ORDER BY COUNT(ba.Id) '
        else:
            ste_tmp += ' ORDER BY AVG(ba.Rating) '

        if command.find('bottom=') != -1:
            num = get_num_or_name(command, 'bottom=')
            ste_tmp += ' ASC LIMIT ?'
            values += (num,)
        else:
            num = 10
            if command.find('top=') != -1:
                num = get_num_or_name(command, 'top=')
            ste_tmp += 'DESC LIMIT ? '
            values += (num,)

        sub_command = command[9:].strip(' ')
        if sub_command != '':
            if sub_command.find('country=') == -1 and sub_command.find('region=') == -1 and sub_command.find('bars_sold') == -1 and sub_command.find('ratings') == -1 and sub_command.find('cocoa') == -1 and sub_command.find('top=') == -1 and sub_command.find('bottom=') == -1:
                return -1

        # write statement for companies
        if command.find('cocoa') != -1:
            statement = 'SELECT ba.Company, co_1.EnglishName, ROUND( AVG(ba.CocoaPercent),2) AS ra FROM Bars AS ba'
        elif command.find('bars_sold') != -1:
            statement = 'SELECT ba.Company, co_1.EnglishName, COUNT(ba.Id) FROM Bars AS ba'
        else:
            statement = 'SELECT ba.Company, co_1.EnglishName, ROUND( AVG(ba.Rating),2) AS rb FROM Bars AS ba'            

        statement += '''
            LEFT JOIN Countries AS co_1
            ON co_1.Id = ba.CompanyLocationId
            '''
        statement += ste_tmp
        # print(statement)
        # print(values)
        cur.execute(statement, values)
        re = []
        for i in cur:
            re.append(i)    
    
    if command[:9] == 'countries':
        ste_tmp = ''
        values = ()

        ste_tmp += 'WHERE co_1.Id IS NOT NULL '
        if command.find('region=') != -1:
            name = get_num_or_name(command, 'region=')
            ste_tmp += ' AND co_1.Region = ? '
            values += (name,)


        ste_tmp += " GROUP BY co_1.Id HAVING COUNT(DISTINCT ba.Id) > 4"

        if command.find('cocoa') != -1:
            ste_tmp += ' ORDER BY AVG(ba.CocoaPercent) '
        elif command.find('bars_sold') != -1:
            ste_tmp += ' ORDER BY COUNT(DISTINCT ba.Id) '
        else:
            ste_tmp += ' ORDER BY AVG(ba.Rating) '

        if command.find('bottom=') != -1:
            num = get_num_or_name(command, 'bottom=')
            ste_tmp += ' ASC LIMIT ?'
            values += (num,)
        else:
            num = 10
            if command.find('top=') != -1:
                num = get_num_or_name(command, 'top=')
            ste_tmp += 'DESC LIMIT ? '
            values += (num,)

        sub_command = command[9:].strip(' ')
        if sub_command != '':
            if sub_command.find('region=') == -1 and sub_command.find('sellers') == -1 and sub_command.find('sources') == -1 and sub_command.find('bars_sold') == -1 and sub_command.find('ratings') == -1 and sub_command.find('cocoa') == -1 and sub_command.find('top=') == -1 and sub_command.find('bottom=') == -1:
                return -1
        # write statement for countries
        if command.find('cocoa') != -1:
            statement = 'SELECT co_1.EnglishName, co_1.Region, ROUND( AVG(ba.CocoaPercent),2) AS ra FROM Countries AS co_1'
        elif command.find('bars_sold') != -1:
            statement = 'SELECT co_1.EnglishName, co_1.Region, COUNT(DISTINCT ba.Id) FROM Countries AS co_1'
        else:
            statement = 'SELECT co_1.EnglishName, co_1.Region, ROUND( AVG(ba.Rating),2) AS rb FROM Countries AS co_1'           
        if command.find('sources') != -1:
            statement += '''
                LEFT JOIN Bars AS ba
                ON co_1.Id = ba.BroadBeanOriginId
                '''            
        else: 
            statement += '''
                LEFT JOIN Bars AS ba
                ON co_1.Id = ba.CompanyLocationId
                '''
        statement += ste_tmp
        # print(statement)
        # print(values)
        cur.execute(statement, values)
        re = []
        for i in cur:
            re.append(i)    

    if command[:7] == 'regions':
        ste_tmp = ''
        values = ()

        ste_tmp += "WHERE co_1.Region IS NOT NULL GROUP BY co_1.Region HAVING COUNT(DISTINCT ba.Id) > 4"

        if command.find('cocoa') != -1:
            ste_tmp += ' ORDER BY AVG(ba.CocoaPercent) '
        elif command.find('bars_sold') != -1:
            ste_tmp += ' ORDER BY COUNT(DISTINCT ba.Id) '
        else:
            ste_tmp += ' ORDER BY AVG(ba.Rating) '

        if command.find('bottom=') != -1:
            num = get_num_or_name(command, 'bottom=')
            ste_tmp += ' ASC LIMIT ?'
            values += (num,)
        else:
            num = 10
            if command.find('top=') != -1:
                num = get_num_or_name(command, 'top=')
            ste_tmp += 'DESC LIMIT ? '
            values += (num,)

        sub_command = command[7:].strip(' ')
        if sub_command != '':
            if sub_command.find('sellers') == -1 and sub_command.find('sources') == -1 and sub_command.find('bars_sold') == -1 and sub_command.find('ratings') == -1 and sub_command.find('cocoa') == -1 and sub_command.find('top=') == -1 and sub_command.find('bottom=') == -1:
                return -1
        # write statement for regions
        if command.find('cocoa') != -1:
            statement = 'SELECT co_1.Region, ROUND( AVG(ba.CocoaPercent),2) AS ra FROM Bars AS ba'
        elif command.find('bars_sold') != -1:
            statement = 'SELECT co_1.Region, COUNT(DISTINCT ba.Id) FROM Bars AS ba'
        else:
            statement = 'SELECT co_1.Region, ROUND( AVG(ba.Rating),2) AS rb FROM Bars AS ba'           
        if command.find('sources') != -1:
            statement += '''
                LEFT JOIN Countries AS co_1
                ON co_1.Id = ba.BroadBeanOriginId
                '''            
        else: 
            statement += '''
                LEFT JOIN Countries AS co_1
                ON co_1.Id = ba.CompanyLocationId
                '''
        statement += ste_tmp
        # print(statement)
        # print(values)
        cur.execute(statement, values)
        re = []
        for i in cur:
            re.append(i)        



    conn.close()
    return re







def load_help_text():
    with open('help.txt') as f:
        return f.read()



# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        print("")
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue
        elif response[:4] == 'bars' or response[:9] == 'companies' or response[:9] == 'countries' or response[:7] == 'regions':
            tuple = process_command(response)
            if tuple == -1:
                print("Command not recognized:", response)
                continue
            # first get max length for each column
            length = []
            for i in range(len(tuple[0])):
                max = -1
                for j in tuple:
                    if len(str(j[i])) > max:
                        max = len(str(j[i]))
                length.append(max)

            for i in tuple:
                for j in range(len(i)):
                    print("%-*s"%(length[j], i[j]), end=" " * 2)
                print("")
            continue
        elif response == 'exit':
            break
        elif response == '' or response == ' ':
            continue
        else:
            print("Command not recognized:", response)
            continue

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    create_db()
    get_data()
    interactive_prompt()

