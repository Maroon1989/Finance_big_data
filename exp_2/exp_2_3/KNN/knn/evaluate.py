import numpy as np
import sklearn.metrics as skm
from sklearn.metrics import accuracy_score,precision_score,classification_report,f1_score,recall_score
from sklearn.metrics import roc_curve,roc_auc_score
from sklearn.metrics import RocCurveDisplay
import matplotlib.pyplot as plt

def plot_auc_roc_curve(y_test, y_pred):
    fpr, tpr, _ = roc_curve(y_test, y_pred)
    roc_display = RocCurveDisplay(fpr=fpr, tpr=tpr).plot()
    roc_display.figure_.set_size_inches(5,5)
    plt.plot([0, 1], [0, 1], color = 'g')
# Plots the ROC curve using the sklearn methods - Good plot
    # plot_auc_roc_curve(y_test, y_proba[:, 1])
# 在这里实现分类以及回归的评价指标

def cal_accuracy(y_pred, y_true):
    return accuracy_score(y_true,y_pred)

def cal_precison(y_pred, y_true):
    return precision_score(y_true,y_pred)

def cal_recall(y_pred, y_true):
    return recall_score(y_true,y_pred)

def cal_f1(y_pred, y_true):
    return f1_score(y_true,y_pred)

def mse(y_pred, y_true, y_info): # 均方误差
    pass

def mae(y_pred, y_true, y_info): # 平均绝对误差
    pass

def rmse(y_pred, y_true, y_info): # 均方根误差
    rmse = skm.mean_squared_error(y_pred, y_true) ** 0.5  
    if y_info['policy'] == 'mean_std':
        rmse *= y_info['std']
    return rmse

def R2(y_pred, y_true, y_info): # 决定系数
    pass

import numpy as np

output_file = '/home/hadoop/Workspace/exp_2/exp_2_1/exp_2_3/KNN/knn/output/part-r-00000'
data = []

with open(output_file, 'r') as file:
    lines = file.readlines()
    for line in lines:
        # 将每行的数值转换为浮点数并添加到列表中
        data.append(float(line.strip()))

# 将列表转换为NumPy数组
data_array = np.array(data)

true_file = '/home/hadoop/Workspace/exp_2/exp_2_1/exp_2_3/KNN/knn/test_original.txt'
last_column_data = []

with open(output_file, 'r') as file:
    lines = file.readlines()
    for line in lines:
        columns = line.strip().split("\t")
        last_column_data.append(float(columns[-1]))
last_column_array = np.array(last_column_data)
print(classification_report(last_column_array,data_array))
print('cal_accuracy:',cal_accuracy(last_column_array,data_array))
print('cal_f1:',cal_f1(last_column_array,data_array))
print('cal_recall:',cal_recall(last_column_array,data_array))
print('cal_precision:',cal_precison(last_column_array,data_array))
print(roc_auc_score(last_column_array,data_array))
plot_auc_roc_curve(last_column_array,data_array)