import utils as u
import pandas as pd

def create_ex_dataframe_1():
    
    df_test1 = pd.DataFrame ({'col1':[1,2,3,3,10]                   
                        ,'col2':[1,2,3,3,2]
                        ,'col3':[2,3,4,4,3]
                        ,'col4':[3,5,7,7,0]
                        ,'col5':[4,5,8,8,1]
                        ,'col6':[2,3,4,4,3]})

    df_test2 = pd.DataFrame ({'col1':[1,2,3,3,10]
                              ,'col2':[1,2,3,3,2]                   
                           })

    df_test3 = pd.DataFrame ({'col1':[1,2,3,3,11],

                           })
    df_test = pd.concat([df_test1,df_test2,df_test3],axis=1)
    
    return df_test

def create_ex_dataframe_2():
    
    df_test1 = pd.DataFrame ({'col1':[1,2,3,3,10]                   
                        ,'col2':[1,2,3,3,2]
                        ,'col3':[2,3,4,4,3]
                        ,'col4':[3,5,7,7,0]
                        ,'col5':[4,5,8,8,1]
                        ,'col6':[2,3,4,4,3]})

    df_test2 = pd.DataFrame ({'col1':[1,2,3,3,10]
                              ,'col2':[1,2,3,3,2]                   
                           })

    df_test = pd.concat([df_test1,df_test2],axis=1)
    
    return df_test


def test_input_df_changed():
    
    df_input = create_ex_dataframe_1()
    
    dup = u.DuplicationCheck(df_input)
    dup.check_column_name_duplication()
    dup.check_same_data_duplication()
    dup.check_row_duplication()
    
    assert df_input.equals(create_ex_dataframe_1()) == True,"Original dataframe is changed"
    
    
def test_same_col_name_removed():
    
    df_input = create_ex_dataframe_2()
    
    dup = u.DuplicationCheck(df_input)
    dup.check_column_name_duplication()
    
    df_expected = pd.DataFrame ({'col1':[1,2,3,3,10]                   
                        ,'col2':[1,2,3,3,2]
                        ,'col3':[2,3,4,4,3]
                        ,'col4':[3,5,7,7,0]
                        ,'col5':[4,5,8,8,1]
                        ,'col6':[2,3,4,4,3]
                        })
    
    assert df_expected.equals(dup.get_dedup_df()) == True,"check_column_name_duplication function doesn't work as expected"
    
    
def test_same_col_name_diff_data_not_removed():
    
    df_input = create_ex_dataframe_1()
    
    dup = u.DuplicationCheck(df_input)
    dup.check_column_name_duplication()
    
    df_expected = pd.DataFrame ({'col1':[1,2,3,3,10]                   
                        ,'col2':[1,2,3,3,2]
                        ,'col3':[2,3,4,4,3]
                        ,'col4':[3,5,7,7,0]
                        ,'col5':[4,5,8,8,1]
                        ,'col6':[2,3,4,4,3]
                        ,'col1_dup_1':[1,2,3,3,11]
                        })
    
    assert df_expected.equals(dup.get_dedup_df()) == True,"check_column_name_duplication function doesn't work as expected"
    

def test_diff_col_name_same_data_removed():
    
    df_input = create_ex_dataframe_1()
    
    dup = DuplicationCheck(df_input)
    dup.check_column_name_duplication()
    dup.check_same_data_duplication()
    
    df_expected = pd.DataFrame ({'col1':[1,2,3,3,10]                   
                        ,'col2':[1,2,3,3,2]
                        ,'col3':[2,3,4,4,3]
                        ,'col4':[3,5,7,7,0]
                        ,'col5':[4,5,8,8,1]
                        ,'col1_dup_1':[1,2,3,3,11]
                        })
    
    assert df_expected.equals(dup.get_dedup_df()) == True,"check_same_data_duplication function doesn't work as expected"
    
    
def test_dup_rows_removed():
    
    df_input = create_ex_dataframe_1()
    
    dup = DuplicationCheck(df_input)
    
    dup.check_column_name_duplication()
    dup.check_same_data_duplication()
    dup.check_row_duplication()
    
    df_expected = pd.DataFrame ({'col1':[1,2,3,10]                   
                        ,'col2':[1,2,3,2]
                        ,'col3':[2,3,4,3]
                        ,'col4':[3,5,7,0]
                        ,'col5':[4,5,8,1]
                        ,'col1_dup_1':[1,2,3,11]
                        },index=[0,1,2,4])
    
    assert df_expected.equals(dup.get_dedup_df()) == True,"check_row_duplication function doesn't work as expected"
    
    