from typing import (    
    List,
    Optional
)
import logging

import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class DuplicationCheck:
    
    """
    This class can be used to check duplication in a dataframe. Duplication can be checked
    based on below aspects
    
    - Are there duplicated column names- ex: two columns have the same column name
      This function checks if the values are the same in both columns and if the values are the
      same, one column will be removed. If the two columns have different values, then the the second 
      column will be renamed
    - Are there two columns that have the same data (with different column names)
      The second column with same data will be removed
    - Duplicated rows based on all the columns are removed
    - if single_cols_check is specified, duplications are checked for rows for each column
      specified in single_cols_check individually. If there are duplicated, they will be removed
    - if multi_cols_dup_check is specified, duplications are checked for rows for the columns group
      specified in multi_cols_dup_check. If there are duplicated, they will be removed
      
    
    """
    
    def __init__(
        self,
        df: pd.DataFrame,
        single_cols_check: Optional[List[str]] = None,
        multi_cols_dup_check: Optional[List[str]] = None,
    ) -> None:
        
        """
        params
        ------
        df : input dataframe
        single_cols_check : Check duplication based on each column individually
        multi_cols_dup_check : Check duplication based on the column group not individually but together
        
        """
    
        self.df = df
        self.single_cols_check = single_cols_check
        self.multi_cols_dup_check = multi_cols_dup_check
        
        self.dup_cols_list = None
        self.df_dedup = None
        self.same_data_cols = None
        self.df_dedup_row = None
    

    @staticmethod
    def get_dup_col_indexes(df: pd.DataFrame,col_name: str) -> List:
    
        """
        This function is used to get the location/index of duplicated columns
        If the given column is note duplicated in the dataset, the  function
        will not work

        params
        ------

        df : dataset
        col_name : name of the column that is duplicated

        returns
        -------
        Indexes of the duplicated columns in a list

        """

        if col_name not in list(df.columns[df.columns.duplicated()]):
            raise KeyError("The input column is not duplicated. to get the index just use get_loc(col_name) ")

        indexes = [i for i,j in enumerate(df.columns.get_loc(col_name)) if j==True]

        return indexes
    

    def check_column_name_duplication(self) -> None:
        
        """
        Check if duplicated column names are present in the dataset
        """
        
        self.df_dedup = self.df.copy()
        
        # This is done to avoid the original input dataframe to remain unchaged when
        #self.df_dedup.columns.values[j] = x+'_dup_'+str(i) is run below
        # even a copy is made above, still the above line renames the original dataframe as well
        self.df_dedup['test'] = 1
        self.df_dedup = self.df_dedup.drop(columns=['test'])
        
        self.dup_cols_list = list(set((self.df_dedup.columns[self.df_dedup.columns.duplicated()])))
       
        dup_names_diff_vals = []
        dup_names_dup_vals = []    
        
        for x in self.dup_cols_list:
          
            duped_cols_indexes = DuplicationCheck.get_dup_col_indexes(self.df_dedup,x)
            
            for i,j in enumerate(duped_cols_indexes[1:]):
                self.df_dedup.columns.values[j] = x+'_dup_'+str(i)
                         
            dup_cols = [col for col in self.df_dedup.columns if x in col]         
            
            for i in range(1,len(dup_cols)):
                equal_flag = self.df_dedup[dup_cols[0]].equals(self.df_dedup[dup_cols[i]])

                if equal_flag:
                    self.df_dedup = self.df_dedup.drop(columns=[dup_cols[i]])
                    dup_names_dup_vals.append(x+': '+dup_cols[i])
                else:
                    dup_names_diff_vals.append(x+': '+dup_cols[i])

        logging.info(f'duplicated columns removed : {dup_names_dup_vals},duplicated column names but different values, These were not removed, need to check manually: {dup_names_diff_vals}')

    def check_same_data_duplication(self) -> None:
        
        """
        Check if duplicated data is present in the dataset with
        different column names
        """
        
        if self.df_dedup is not None: 
            
            all_cols = list(self.df_dedup)
        
            self.same_data_cols = []

            for i in all_cols:
                for j in all_cols:
                    if i != j:
                        equal_flag = self.df_dedup[i].equals(self.df_dedup[j])

                        if equal_flag:
                            if j+' '+i not in self.same_data_cols:
                                self.same_data_cols.append(i+' '+j) 
        
            for i in self.same_data_cols:
                
                remove_col_name = i.split(' ')[1] #remove the second column with same data
                try:
                    self.df_dedup = self.df_dedup.drop(columns=[remove_col_name])
                except KeyError:
                    pass
                logging.info(f"column {remove_col_name} removed becasue it has same data as { i.split(' ')[0]}")
        
        else:
            raise AttributeError("Need to run 'check_column_name_duplication' function first")
               
        
    def get_duplicated_col_name(self) -> List:
        
        if self.dup_cols_list is not None:      
            return  self.dup_cols_list
        else:
            raise AttributeError("Need to run 'check_column_name_duplication' function first")

            
    def get_dedup_df(self) -> pd.DataFrame:
        
        """
        Get the deduped dataset
        """
        
        if self.df_dedup_row is not None:
            return self.df_dedup_row
        
        elif self.df_dedup is not None:    
            return  self.df_dedup
        else:
            raise AttributeError("Need to run 'check_column_name_duplication' function first")
            
    def get_duplicated_data_col_names(self) -> List:
        
        """
        Get the duplicated column names
        """
        
        if self.same_data_cols is not None:      
            return  self.same_data_cols
        else:
            raise AttributeError("Need to run 'check_same_data_duplication' function first")
            
            
    def check_row_duplication(self) -> None:
        
        """
        Check duplicates for rows
        """
        
        if self.df_dedup is not None:           
            # on all columns
            self.df_dedup_row = self.df_dedup.drop_duplicates(keep='first') 
            all_n_rows = len(self.df_dedup)
            all_n_rows_deduped = len(self.df_dedup_row)
            
            if all_n_rows > all_n_rows_deduped:
                logging.info(f"{all_n_rows-all_n_rows_deduped} rows removed due to row duplication based on all columns")
            else:
                 logging.info("No row duplication on all columns")
                    
            if self.single_cols_check is not None:
                
                all_n_rows = len(self.df_dedup_row)
                
                for i in self.single_cols_check:
                    self.df_dedup_row = self.df_dedup_row.drop_duplicates(subset=[i],keep='first') 
                
                all_n_rows_deduped = len(self.df_dedup_row)
                
                if all_n_rows > all_n_rows_deduped:
                    logging.info(f"{all_n_rows-all_n_rows_deduped} rows removed due to row duplication based specified single columns")
                else:
                     logging.info("No row duplication based on specified columns")
                        
            if self.multi_cols_dup_check is not None:
                
                all_n_rows = len(self.df_dedup_row)          
               
                self.df_dedup_row = self.df_dedup_row.drop_duplicates(subset=self.multi_cols_dup_check,keep='first') 
                
                all_n_rows_deduped = len(self.df_dedup_row)
                
                if all_n_rows > all_n_rows_deduped:
                    logging.info(f"{all_n_rows-all_n_rows_deduped} rows removed due to row duplication based specified column group")
                else:
                     logging.info("No row duplication based on specified column group") 
                                  
        else:
            raise AttributeError("Need to run 'check_column_name_duplication' function first")