import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class Olist:
    def get_data(self):
        parent = os.path.abspath(os.path.join(__file__,os.pardir))
        gp=os.path.abspath(os.path.join(parent,os.pardir))
        csv_path=os.path.join(gp,'raw_data/csv')

        file_names=os.listdir(csv_path)

        key_names=[]
        for file in file_names:
            key_names.append(file.replace("olist_","").replace("_dataset.csv","").replace(".csv",""))

        data={}
        for file in key_names:
            if file=='product_category_name_translation':
                dataframe=pd.read_csv(os.path.join(csv_path, f'{file}.csv'))
                data[f'{file}']=dataframe
            else:
                dataframe=pd.read_csv(os.path.join(csv_path, f'olist_{file}_dataset.csv'))
                data[f'{file}']=dataframe
        return data
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """
        # Hints 1: Build csv_path as "absolute path" in order to call this method from anywhere.
            # Do not hardcode your path as it only works on your machine ('Users/username/code...')
            # Use __file__ instead as an absolute path anchor independant of your usename
            # Make extensive use of `breakpoint()` to investigate what `__file__` variable is really
        # Hint 2: Use os.path library to construct path independent of Mac vs. Unix vs. Windows specificities
        # YOUR CODE HERE

    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")

    def annot_max(self,x,y, ax=None):
        xmax = x[np.argmax(y)]
        ymax = y.max()
        text= "Number of seller removed={:.0f}, Total Profit={:.2f}".format(xmax, ymax)
        if not ax:
            ax=plt.gca()
        bbox_props = dict(boxstyle="square,pad=0.40", fc="w", ec="k", lw=0.72)
        arrowprops=dict(arrowstyle="->",connectionstyle="angle,angleA=0,angleB=60")
        kw = dict(xycoords='data',textcoords="axes fraction",arrowprops=arrowprops, bbox=bbox_props, ha="right", va="top")
        ax.annotate(text, xy=(xmax, ymax), xytext=(1.5,1.25), **kw)
