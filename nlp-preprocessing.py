import numpy as np


def one_hot_encode_target(target_array: np.array, 
                          num_samples: int,
                          num_classes: int
) -> np.array:
    
    """
    To build a neural network for a classification problem,
    the target needs to one-hot encoded. This function
    can be used to one-hot encode the target
    
    params
    ------
    
    target_array : targets for each row 
    num_samples : number of rows in the dataset
    num_classes : number of different classes in the classfication problem
    
    """
    
    labels = np.zeros((num_samples,num_classes))
    labels[np.arange(num_samples), target_array] = 1
    
    return labels