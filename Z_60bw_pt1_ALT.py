print("importing modules")
import pandas as pd
from bs4 import BeautifulSoup
from cryptography.fernet import Fernet
import openpyxl
from Z_60bscript_logger import ghetto_logger


class ClashWork:
    def __init__(self, config):
        self.log=ghetto_logger("60b_pt1_ALT.py")
    def import_data(self):
        '''importing data into pandas dataframe'''
        self.log.log("importing data")
        self.clash_id_path = r"C:\Egnyte\Shared\Digital Construction Team\00_Projects\FL_60 Blossom Way\08_Working\Clash\Current Clash Report and Dynamo Assets\clash_id - ALT.xlsx"
        self.import_processed_path = r"C:\Egnyte\Shared\Digital Construction Team\00_Projects\FL_60 Blossom Way\08_Working\Clash\Current Clash Report and Dynamo Assets\Import_Processed.xlsx"
        self.excel_source = pd.read_excel(r"C:\Egnyte\Shared\Digital Construction Team\00_Projects\FL_60 Blossom Way\08_Working\Clash\Current Clash Report and Dynamo Assets\Import.xlsx", header=1)
        self.model_key = pd.read_excel(r"C:\Egnyte\Shared\Digital Construction Team\00_Projects\FL_60 Blossom Way\08_Working\Clash\Current Clash Report and Dynamo Assets\model_key.xlsx",engine="openpyxl")
        self.clash_id = pd.read_excel(self.clash_id_path,engine="openpyxl")
    #region import excel transformations
    def clean_excel_source(self):
        '''need to start by making the df readable (I just need columns 14+, and their titles are on row 5)'''
        df=self.excel_source

        #region OLD
        # columns 0 through 12 are meta data for the clash
        # df = df.drop(columns=df.columns[0:13])

        # Rename columns based on row 5
        # new_col_names = [str(df.iloc[5, col]) for col in range(0, 14)] + \
        #                 ["Item 1 - " + str(df.iloc[5, col]) for col in range(14, 19)] + \
        #                 ["Item 2 - " + str(df.iloc[5, col]) for col in range(19, 25)] + \
        #                 [str(df.iloc[5, 26])]
        #endregion

        # Building the new column names list
        row_data = df.iloc[5]

        # Building the new column names list
        new_col_names = [
            row_data[col] if pd.notna(row_data[col]) else 'BLANK' for col in range(0, 13)
        ] + [
            "Item 1 - " + row_data[col] for col in range(13, 19)
        ] + [
            "Item 2 - " + row_data[col] for col in range(19, 25)
        ] + [
            row_data[25]
        ]
        

        df.columns = new_col_names

        # Drop rows 0-5
        df = df.drop(index=range(6))

        # Reset index if needed
        df = df.reset_index(drop=True)

        # remove NaNs for later work w/ ""
        df = df.fillna("")

        self.excel_source = df
    def replace_path(self, input_string):
        '''needed to make sure the model_key is recognized. Its tricky b/c the model key is based on this type of input: 
        nwbim360://developer.api.autodesk.com/coord/v1/region/us/account/53193af9-42b4-4018-86f8-0e3f265a1dad/project/f0eeb601-51e2-4888-86e4-d90893cb98d8/container/f0eeb601-51e2-4888-86e4-d90893cb98d8/modelset/444bec68-629a-4412-8717-843473307b8c/model/dXJuOmFkc2sud2lwcHJvZDpkbS5saW5lYWdlOk41WVZPTEZJUXBtZC1Ub0pDa3FQT3c/name/TWVjaGFuaWNhbA.svf?explicit=true&type=.svf
        which always ends ... name/TWVjaGFuaWNhbA.svf?explicit=true&type=.svf
        but now DCPEs are using egnyte, such as C:\Egnyte\Shared\Digital Construction Team\00_Projects\FL_60 Blossom Way\01_Revit\WRNash Models\WRN- 60BW Plumbing.rvt
        which we need to artificially end name/<last subsection of string after last slash>'''
        adjusted_string = input_string
        # If 'egnyte' is in the string after converting to lowercase
        if 'egnyte' in input_string.lower():
            # Find the last occurrence of "\"
            last_slash_index = input_string.rfind("\\")
            # If "\" was found
            if last_slash_index != -1:
                # Replace the last "\" with "\name\"
                adjusted_string = input_string[:last_slash_index] + "\\name/" + input_string[(last_slash_index+1):]

        return adjusted_string
    def transform_excel_source(self):
        '''Daniel requested that we extract the Element ID from the Item Id column, and add it to the Item Name column as <Item File Name> - <Element ID #>, we do so here for both items'''
        item_list = ['Item 1', 'Item 2']
        df=self.excel_source

        for item_number in item_list:
            item_id_col = f'{item_number} - Item ID'
            item_name_col = f'{item_number} - Item Name'
            item_file_name_col = f'{item_number} - Item File Name'

            # Finds rows w elemnt
            mask = df[item_id_col].str.contains('Element ID:', na=False)
            df.loc[mask, item_name_col] = df[item_name_col] + ' [' + df[item_id_col].str.extract('Element ID: (\d+)')[0] + ']'
            
            
            # Apply the function on your DataFrame column
            df[item_file_name_col] = df[item_file_name_col].apply(self.replace_path)

            # # adds name/ to the file name so that in discipline extractoin it can extract the key descriptor out
            # df[item_file_name_col] = df[item_file_name_col].str.replace("60BW_Plumbing_WRNash", "name/60BW_Plumbing_WRNash")
    def transformation_audit(self):
        '''check that it worked'''
        df = self.excel_source
        filtered_rows = df[df['Item 2 - Item ID'].str.contains('Element ID', na=False)]

        for value in filtered_rows['Item 2 - Item Name']:
            print(value)
    def post_processed_excel(self):
        '''for auditing purposes, post to the results to a new excel'''
        self.log.log('posting to Import Processed')
        self.excel_source.to_excel(self.import_processed_path, index=False, engine='openpyxl')
    def process_excel_source(self):
        '''run data processing on the source excel b/c the imports were not coming through correctly on the html'''
        self.log.log('processing import data...')
        self.clean_excel_source()
        self.transform_excel_source()
        # self.transformation_audit()
        self.post_processed_excel()
    #endregion
    def extract_source_values(self):
        '''extracts values from source list of dataframes'''
        self.log.log("extracting values")
        uid_list = []
        df = self.excel_source
        try:
            uid = df['Item 1 - Item Name'].str.extract(r'\[(\d+)\]') + "-" + df['Item 2 - Item Name'].str.extract(r'\[(\d+)\]')
            uid_list.extend(uid[0].values.tolist())
        except KeyError:
            pass
        return uid_list
    def clean_model_key(self, str):
        '''DCPEs keep entering whole file names into the Model_key instead of what Torrie wanted (the section after last slash)
        so this script returns just what Torrie designed to have returned'''
        # If 'egnyte' is in the string after converting to lowercase
        if 'egnyte' in str.lower():
            # Split the string by "\" and take the last part
            return str.split("\\")[-1]
        return str  # Return the original string if 'egnyte' is not present
    def extract_discipline(self, column_name):
        '''extracts the discipline from one isolated component'''
        self.failed_indexes = []
        df = self.excel_source
        model_key_list = [self.clean_model_key(name) for name in self.model_key['Navis Source File Name'].values.tolist()]
        try:
            file_names_df = df[column_name].str.extract(r'name/(.+)').reset_index()
            file_names_df.columns = ['original_index', 'file_name']
        except KeyError:
            pass
        discipline_list = []
        # for index, file in enumerate(file_name_list):
        for _, row in file_names_df.iterrows():
            index = row['original_index']
            file = row['file_name']
            if file in model_key_list:
                # map value to model_key, by finding index of model_key_list item (cleaned by clean_model_key) and then grabbing the code of that index in model key .xlsx
                index = model_key_list.index(file)
                code = self.model_key.loc[index]['Code']
                discipline_list.append(code)
            else:
                self.failed_indexes.append(index)
                self.file = file
                discipline_list.append('X')
        return discipline_list
    def process_discipline(self, uid_list):
        '''creates a humanly-readable discipline by working with each component that is clashing and generating the final coding of the clash discpline'''
        discipline_a = self.extract_discipline('Item 1 - Item File Name')
        discipline_b = self.extract_discipline('Item 2 - Item File Name')
        discipline_combined = [a + b for a,b in zip(discipline_a, discipline_b)]
        uid_table = pd.DataFrame(list(zip(uid_list, discipline_combined)), columns=['uid','discipline'])
        return uid_table
    def find_max_clashid(self):
        '''finds the existing max clash_id so the new ones can be appropriately named after it'''
        try:
            # the plus one solves a bug where the first new value would start at the SAME clash id as the previous max clash.
            self.max_clashid = max(cw.clash_id['clash_id'].values.tolist()) + 1
        except ValueError:
            self.max_clashid = 0
    def register_ids(self, uid_table):
        '''puts all data together into a single list of dict records, returns the list ready to become a df using pd.DataFrame.from_records()
        works by going through existing data, reappending if not duplicate, and then going through incomming data, and appending onto end if its unique'''

        unique_uid_list= [] 
        incoming_uid_list = uid_table.uid.to_list()
        existing_uid_list = self.clash_id.uid.to_list()
        existing_records = self.clash_id.to_dict(orient='records')
        clash_data_list= []
        current_index = 0
        
        # reregister existing non-duplicates
        for i, uid in enumerate(existing_uid_list):
            if uid in unique_uid_list:
                # consider indexes later.....
                current_index = current_index + 1
                pass 
            else:
                new_row = {'uid': uid, 'discipline':existing_records[i].get('discipline'), 'clash_id':existing_records[i].get('clash_id'), 'index': current_index}
                clash_data_list.append(new_row)
                current_index = current_index + 1
                
                #add this uid to the uinque_uid_list so we don't duplicate any
                unique_uid_list.append(uid)
                unique_uid_list.append(uid.split('-').reverse())

        self.log.log('the following rows are being newly added:')
        # register new ones at the bottom, with a clash_id of max_clashid + new_uid_index
        for new_uid_index, uid in enumerate(incoming_uid_list):
            # checks if UID is already in excel
            if uid in unique_uid_list:
                # consider indexes later.....
                current_index = current_index + 1
                pass
            else:
                try:
                    discipline_var = uid_table.loc[uid_table.uid == uid]['discipline'].values.tolist()[0]
                except IndexError:
                    discipline_var = ""
                else:
                    # clash_id = max clash + index of new
                    new_row = {'uid': uid, 'discipline':discipline_var, 'clash_id':f'{(self.max_clashid+new_uid_index):05d}', 'index': current_index}
                    current_index = current_index + 1
                    self.log.log(str(new_row))
                    clash_data_list.append(new_row)
                    
                    #add this uid to the uinque_uid_list so we don't duplicate any
                    unique_uid_list.append(uid)
                    unique_uid_list.append(uid.split('-').reverse())
        
        return clash_data_list
    def post_dict_toexcel(self, clash_dict):
        '''posts to excel'''
    
        clash_id_new = pd.DataFrame.from_records(clash_dict)
        self.df = clash_id_new
        try:
            clash_id_new['clash_id'] = clash_id_new.clash_id.apply(lambda x: f'{int(x):05d}')
        except AttributeError:
            pass

        clash_id_new.to_excel(self.clash_id_path, engine="openpyxl", index=False)

        print("Successfully Completed!")
        import time
        time.sleep(2)

    def run(self):
        self.import_data()
        self.process_excel_source()
        self.uid_list = self.extract_source_values()
        self.uid_table = self.process_discipline(self.uid_list)
        self.find_max_clashid()
        self.clash_dict = self.register_ids(self.uid_table)
        self.post_dict_toexcel(self.clash_dict)

if __name__ == "__main__":
    config = {
        '':'',
    }
    cw = ClashWork(config)
    cw.run()

