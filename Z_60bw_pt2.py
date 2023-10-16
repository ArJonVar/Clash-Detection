import pandas as pd
import smartsheet
from datetime import datetime
from smartsheet.exceptions import ApiError
import pandas as pd
from Z_60bscript_logger import ghetto_logger
from cryptography.fernet import Fernet
import os

class grid:

    """
    Global Variable
    ____________
    token --> MUST BE SET BEFORE PROCEEDING. >>> grid.token = {SMARTSHEET_ACCES_TOKEN}

    Dependencies
    ------------
    smartsheet as smart (smartsheet-python-sdk)
    pandas as pd

    Attributes
    __________
    grid_id: int
        sheet id of an existing Smartsheet sheet. terst 1

    Methods
    -------
    grid_id --> returns the grid_id
    grid_content ---> returns the content of a sheet as a dictionary.
    grid_columns ---> returns a list of the column names.
    grid_rows ---> returns a list of lists. each sub-list contains all the 'display values' of each cell in that row.
    grid_row_ids---> returns a list o
    f all the row ids
    grid_column_ids ---> returns a list of all the column ids
    df ---> returns a pandas DataFrame of the sheet.
    delete_all_rows ---> deletes all rows in the sheet (in preperation for updating).

    """

    token = None

    def __init__(self, grid_id):
        self.grid_id = grid_id
        self.grid_content = None
        self.column_df = self.get_column_df()
    
    def get_column_df(self):
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            smart = smartsheet.Smartsheet(access_token=self.token)
            smart.errors_as_exceptions(True)
            return pd.DataFrame.from_dict(
            (smart.Sheets.get_columns(self.grid_id, level=2, include='objectValue', include_all=True)).to_dict().get("data")
        )

    def df_id_by_col(self, column_names):
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            smart = smartsheet.Smartsheet(access_token=self.token)
            smart.errors_as_exceptions(True)
            columnids = []
            col_index = []
            for col in column_names:
                col1 = smart.Sheets.get_column_by_title(self.grid_id, col)
                columnids.append(col1.to_dict().get("id"))
                col_index.append(col1.to_dict().get("index"))
            sorted_col = [x for y, x in sorted(zip(col_index, column_names))]
            sfetch = smart.Sheets.get_sheet(self.grid_id, column_ids=columnids)
            cols = ["id"] + sorted_col
            c = []
            p = sfetch.to_dict()
            for i in p.get("rows"):
                l = []
                l.append(i.get("id"))
                for i in i.get("cells"):
                    l.append(i.get("displayValue"))
                c.append(l)
            return pd.DataFrame(c, columns=cols)

    def fetch_content(self):
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            smart = smartsheet.Smartsheet(access_token=self.token)
            smart.errors_as_exceptions(True)
            self.grid_content = (smart.Sheets.get_sheet(self.grid_id)).to_dict()
            self.grid_name = (self.grid_content).get("name")
            # this attributes pulls the column headers
            self.grid_columns = [i.get("title") for i in (self.grid_content).get("columns")]
            # note that the grid_rows is equivelant to the cell's 'Display Value'
            self.grid_rows = []
            if (self.grid_content).get("rows") == None:
                self.grid_rows = []
            else:
                for i in (self.grid_content).get("rows"):
                    b = i.get("cells")
                    c = []
                    for i in b:
                        l = i.get("displayValue")
                        m = i.get("value")
                        if l == None:
                            c.append(m)
                        else:
                            c.append(l)
                    (self.grid_rows).append(c)
            self.grid_rows = self.grid_rows
            if (self.grid_content).get("rows") == None:
                self.grid_row_ids = []
            else:
                self.grid_row_ids = [i.get("id") for i in (self.grid_content).get("rows")]
            self.grid_column_ids = [i.get("id") for i in (self.grid_content).get("columns")]
            self.df = pd.DataFrame(self.grid_rows, columns=self.grid_columns)
            self.df["id"]=self.grid_row_ids
            
    def fetch_summary_content(self):
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            smart = smartsheet.Smartsheet(access_token=self.token)
            smart.errors_as_exceptions(True)
            self.grid_content = (smart.Sheets.get_sheet_summary_fields(self.grid_id)).to_dict()
            # this attributes pulls the column headers
            self.summary_params=[ 'title','createdAt', 'createdBy', 'displayValue', 'formula', 'id', 'index', 'locked', 'lockedForUser', 'modifiedAt', 'modifiedBy', 'objectValue', 'type']
            self.grid_rows = []
            if (self.grid_content).get("data") == None:
                self.grid_rows = []
            else:
                for summary_field in (self.grid_content).get("data"):
                    row = []
                    for param in self.summary_params:
                        row_value = summary_field.get(param)
                        row.append(row_value)
                    self.grid_rows.append(row)
            if (self.grid_content).get("rows") == None:
                self.grid_row_ids = []
            else:
                self.grid_row_ids = [i.get("id") for i in (self.grid_content).get("data")]
            self.df = pd.DataFrame(self.grid_rows, columns=self.summary_params)
    
    def reduce_columns(self,exclusion_string):
        """a method on a grid{sheet_id}) object
        take in symbols/characters, reduces the columns in df that contain those symbols"""
        if self.token == None:
            return "MUST SET TOKEN"
        else:
            smart = smartsheet.Smartsheet(access_token=self.token)
            smart.errors_as_exceptions(True)
            regex_string = f'[{exclusion_string}]'
            self.column_reduction =  self.column_df[self.column_df['title'].str.contains(regex_string,regex=True)==False]
            self.reduced_column_ids = list(self.column_reduction.id)
            self.reduced_column_names = list(self.column_reduction.title)

class Clashlog_maintainer:
    '''description pending'''
    def __init__(self, config):
        self.log=ghetto_logger("Z_60bw_pt2.py")
        raw_now = datetime.now()
        self.now = raw_now.strftime("%m/%d/%Y %H:%M:%S")
        self.smartsheet_token = config.get("ss_api_token")
        grid.token=self.smartsheet_token
        self.smart = smartsheet.Smartsheet(access_token=self.smartsheet_token)
        self.smart.errors_as_exceptions(True)
        self.sheet_id=config.get("ss_clashlog_sheetid")
        self.log.log("pulling excel data...")
        self.xlsx_df = pd.read_excel(config.get("sys_path_to_excel_clash"))

    #region columns
    def get_column_names(self):
        self.log.log("pulling smartsheet data...")
        df = grid(self.sheet_id)
        df.fetch_content()
        ss_columns = df.grid_columns
        self.column_df = df.column_df
        return ss_columns, df.df 

    def get_column_id(self, column_name):
        return list(self.column_df.loc[self.column_df['title'] == column_name]['id'].values)[0]
    
    def find_common_columns(self, xl, ss):
        common_columns = [column for column in xl if column in ss]
        col_post_data = [{"str":column, "id":self.get_column_id(column)} for column in common_columns]
        return col_post_data
    #endregion

    #region rows
    def clean_list(self, mylist):
        '''makes each item string, removes duplicates, none, and nans'''
        str_list = list(map(str, mylist))
        unique_list = list(dict.fromkeys(str_list))
        try:
            unique_list.remove("None")
        except:
            pass
        try:
            unique_list.remove("nan")
        except:
            pass
        return unique_list

    def id_processing(self, xlsx_ids, ss_ids, df):
        '''if exists in XL but not SS, add row to bottom, if exists in SS but not CL, change to status closed'''
        self.log.log("comparing excel and smartsheet data...")
        add_row = [xlid for xlid in xlsx_ids if xlid not in ss_ids]
        # only needs to be added to status_closed if current status is not closed...
        status_closed = [ssid for ssid in ss_ids if ssid not in xlsx_ids and list(df.loc[df['Clash ID'] == ssid]['Status'].values)[0] != "Closed"]
        
        self.log.log(f'''  -{len(status_closed)} row modifications needed, 
  -{len(add_row)} row additions needed''')
        
        return add_row, status_closed

    def find_row_id(self, value, df):
        return list(df.loc[df['Clash ID'] == value]['id'].values)[0]
    
    def modify_ss_row(self, ids, df):
        self.log.log(f"modifying {len(ids)} existing rows...")
        mass_post = []
        for i, id in enumerate(ids):
            if (int(i)/100).is_integer() == True and int(i) != 0:
                self.log.log(f"  {i} rows modified...")
            if str(id) != "None":
                row_id = self.find_row_id(id, df)
                new_row = smartsheet.models.Row()
                new_row.id = int(row_id)

                new_cell = smartsheet.models.Cell()
                new_cell.column_id = int(self.get_column_id('Status'))
                new_cell.value = "Closed"
                new_cell.strict = False
                new_row.cells.append(new_cell)

                mass_post.append(new_row)

        updated_row = self.smart.Sheets.update_rows(
            self.sheet_id,      # sheet_id
            mass_post)
        if updated_row.message == "SUCCESS":
            self.log.log(f"-> row modification complete <-")
        else:
            self.log.log("-> row modification error occured <-")
  
    def find_empty_rows(self, df):
        '''this is used to find the first blank row (with no value in Clash ID) so I can start posting new rows above this one and keep all rows together'''
        text_empty = df['Clash ID'].str.len() > -1
        index = [i for i, item in enumerate(list(text_empty.values)) if item == False]
        if len(index) != 0:
            starting_point = df.iloc[index[0]]['id']
        else:
            starting_point = "none"
        return starting_point

    def add_ss_rows(self, ids, col_post_data, start):
        self.log.log(f"processing {len(ids)} new rows... (please hold)")
        mass_post = []
        
        for i, id in enumerate(ids):
            if (int(i)/100).is_integer() == True and int(i) != 0:
                self.log.log(f"  {i} rows processed...")
            elif int(i) > 100 and int(i) == len(ids):
                self.log.log(f"  {len(ids) % 100} rows processed...")
            if str(id) != "nan":
                # Specify cell values for one row
                new_row = smartsheet.models.Row()
                if start == "none":
                    new_row.to_bottom=True
                else:
                    # posts new rows above this specific row (that has the first blank value in Clash ID)
                    new_row.sibling_id = int(start)
                    new_row.above = True

                for col in col_post_data:
                    value = list(self.xlsx_df.loc[self.xlsx_df['Clash ID'] == id][col.get('str')].values)[0]
                    if str(value) != "nan":
                        new_row.cells.append({
                            'column_id': int(col.get("id")),
                            'value': str(value), 
                            'strict': False
                        })

                mass_post.append(new_row)

        # Add rows to sheet
        self.log.log("posting all rows now...")
        row_creation = self.smart.Sheets.add_rows(
          self.sheet_id,       # sheet_id
          mass_post)
        if row_creation.message == "SUCCESS":
            self.log.log(f"-> row additions complete <-")
        else:
            self.log.log("-> row addition error occured <-")
    #endregion

    def run(self):
        ss_columns, ss_gen_df = self.get_column_names()
        xlsx_columns = list(self.xlsx_df.columns.values)
        xlsx_ids = self.clean_list(list(self.xlsx_df['Clash ID'].values))
        ss_ids=self.clean_list(list(ss_gen_df['Clash ID'].values))
        add_row, status_closed = self.id_processing(xlsx_ids, ss_ids, ss_gen_df)
        if len(status_closed) > 0:
            self.modify_ss_row(status_closed, ss_gen_df)
        col_post_data = self.find_common_columns(xlsx_columns, ss_columns)
        starting_point = self.find_empty_rows(ss_gen_df)
        if len(add_row) > 0:
            self.add_ss_rows(add_row, col_post_data, starting_point)
        self.log.log("-> Script Completed <-")

#region name=main
if __name__ == "__main__":
    #get api token from encrypted enviromental variable
    f = Fernet(os.environ.get("AutoK"))
    sensative_smartsheet_token = f.decrypt(bytes(os.environ.get('AutoT'), 'utf-8')).decode("utf-8")

    # the inputs are api key, sheet id for clashlog, and path to xlsx file (from .txt)
    config = {"ss_api_token":sensative_smartsheet_token, 
             "ss_clashlog_sheetid":3988054730925956, 
             "sys_path_to_excel_clash":r'C:\Egnyte\Shared\Digital Construction Team\00_Projects\FL_60 Blossom Way\08_Working\Clash\Current Clash Report and Dynamo Assets\Clash Log.xlsx'}
    clm = Clashlog_maintainer(config)
    clm.run()
#endregion
