import pandas as pd
import numpy as np
from Olistecommerce.utils import haversine_distance
from Olistecommerce.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        orders = self.data['orders'].copy()
        orders=orders[orders['order_status']=='delivered']

        #convert all datetime related columns to datetime
        orders['order_purchase_timestamp']=pd.to_datetime(orders['order_purchase_timestamp'])
        orders['order_delivered_carrier_date']=pd.to_datetime(orders['order_delivered_carrier_date'])
        orders['order_approved_at']=pd.to_datetime(orders['order_approved_at'])
        orders['order_delivered_customer_date']=pd.to_datetime(orders['order_delivered_customer_date'])
        orders['order_estimated_delivery_date']=pd.to_datetime(orders['order_estimated_delivery_date'])

        #to calculated all 3 columns
        orders['wait_time']=orders['order_delivered_customer_date']-orders['order_purchase_timestamp']
        orders['expected_wait_time']=orders['order_estimated_delivery_date']-orders['order_purchase_timestamp']
        orders['delay_vs_expected']=orders['order_delivered_customer_date']-orders['order_estimated_delivery_date']

        # To convert wait_time, expected_wait_time, delay_vs_expected columns to float64 (days)
        orders['wait_time']=orders['wait_time']/pd.to_timedelta(1, unit='D')
        orders['expected_wait_time']=orders['expected_wait_time']/pd.to_timedelta(1, unit='D')
        orders['delay_vs_expected']=orders['delay_vs_expected']/pd.to_timedelta(1, unit='D')

        # To assign delay_vs_expected day to 0 if the value is negative (meaning package arrive earlier than estimated)
        orders["delay_vs_expected"] = [0 if ele<0 else ele for ele in orders["delay_vs_expected"]]

        ans=orders[['order_id','wait_time', 'expected_wait_time', 'delay_vs_expected', 'order_status']]
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        return ans

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        reviews = self.data['order_reviews'].copy()
        assert(reviews.shape == (99224,7))
        reviews['dim_is_five_star']=reviews['review_score'].map({5:1,4:0,3:0,2:0,1:0})
        reviews['dim_is_one_star']=reviews['review_score'].map({5:0,4:0,3:0,2:0,1:1})
        ans=reviews[['order_id', 'dim_is_five_star', 'dim_is_one_star', 'review_score']]
        return ans  # YOUR CODE HERE

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        order_items = self.data['order_items'].copy()
        order_items_gb_orderID=order_items.groupby('order_id').count()
        num_product=order_items_gb_orderID.rename(columns={'order_item_id':'number_of_products'})[['number_of_products']].reset_index()
        return num_product

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        order_items = self.data['order_items'].copy()
        order_item_gb_order=order_items.groupby('order_id').nunique()
        seller_per_order=order_item_gb_order.rename(columns={'seller_id':'number_of_sellers'})[['number_of_sellers']].reset_index()
        return seller_per_order

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        order_items = self.data['order_items'].copy()
        price_freight_value=order_items[['order_id', 'price','freight_value']]
        ans=price_freight_value.groupby('order_id').agg({'price':'sum','freight_value':'sum'}).reset_index()
        return ans  # YOUR CODE HERE

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        # $CHALLENGIFY_BEGIN

        # import data
        data = self.data
        orders = data['orders']
        order_items = data['order_items']
        sellers = data['sellers']
        customers = data['customers']

        # Since one zip code can map to multiple (lat, lng), take the first one
        geo = data['geolocation']
        geo = geo.groupby('geolocation_zip_code_prefix',
                          as_index=False).first()

        # Merge geo_location for sellers
        sellers_mask_columns = [
            'seller_id', 'seller_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'
        ]

        sellers_geo = sellers.merge(
            geo,
            how='left',
            left_on='seller_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[sellers_mask_columns]

        # Merge geo_location for customers
        customers_mask_columns = ['customer_id', 'customer_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']

        customers_geo = customers.merge(
            geo,
            how='left',
            left_on='customer_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[customers_mask_columns]

        # Match customers with sellers in one table
        customers_sellers = customers.merge(orders, on='customer_id')\
            .merge(order_items, on='order_id')\
            .merge(sellers, on='seller_id')\
            [['order_id', 'customer_id','customer_zip_code_prefix', 'seller_id', 'seller_zip_code_prefix']]

        # Add the geoloc
        matching_geo = customers_sellers.merge(sellers_geo,
                                            on='seller_id')\
            .merge(customers_geo,
                   on='customer_id',
                   suffixes=('_seller',
                             '_customer'))
        # Remove na()
        matching_geo = matching_geo.dropna()

        matching_geo.loc[:, 'distance_seller_customer'] =\
            matching_geo.apply(lambda row:
                               haversine_distance(row['geolocation_lng_seller'],
                                                  row['geolocation_lat_seller'],
                                                  row['geolocation_lng_customer'],
                                                  row['geolocation_lat_customer']),
                               axis=1)
        # Since an order can have multiple sellers,
        # return the average of the distance per order
        order_distance =\
            matching_geo.groupby('order_id',
                                 as_index=False).agg({'distance_seller_customer':
                                                      'mean'})

        return order_distance

    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        get_wait_time=self.get_wait_time(is_delivered=True)
        get_review_score=self.get_review_score()
        get_number_products=self.get_number_products()
        get_number_sellers=self.get_number_sellers()
        get_price_and_freight=self.get_price_and_freight()

        clean_df=get_wait_time.merge(get_review_score,how='inner', on='order_id')
        clean_df=clean_df.merge(get_number_products,how='inner', on='order_id')
        clean_df=clean_df.merge(get_number_sellers,how='inner', on='order_id')
        clean_df=clean_df.merge(get_price_and_freight,how='inner', on='order_id')

        if with_distance_seller_customer:
            clean_df = clean_df.merge(
                self.get_distance_seller_customer(), on='order_id')
        clean_df.dropna(axis=0,inplace=True)

        return clean_df
        # Hint: make sure to re-use your instance methods defined above
