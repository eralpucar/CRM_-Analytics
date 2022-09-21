###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

# 1. İş Problemi (Business Problem)
# 2. Veriyi Anlama (Data Understanding)
# 3. Veri Hazırlama (Data Preparation)
# 4. RFM Metriklerinin Hesaplanması (Calculating RFM Metrics)
# 5. RFM Skorlarının Hesaplanması (Calculating RFM Scores)
# 6. RFM Segmentlerinin Oluşturulması ve Analiz Edilmesi (Creating & Analysing RFM Segments)
# 7. Tüm Sürecin Fonksiyonlaştırılması

###############################################################
# 1. İş Problemi (Business Problem)
###############################################################

# Bir e-ticaret şirketi müşterilerini segmentlere ayırıp bu segmentlere göre
# pazarlama stratejileri belirlemek istiyor.

# Veri Seti Hikayesi
# https://archive.ics.uci.edu/ml/datasets/Online+Retail+II

# Online Retail II isimli veri seti İngiltere merkezli online bir satış mağazasının
# 01/12/2009 - 09/12/2011 tarihleri arasındaki satışlarını içeriyor.

# Değişkenler
#
# InvoiceNo: Fatura numarası. Her işleme yani faturaya ait eşsiz numara. C ile başlıyorsa iptal edilen işlem.
# StockCode: Ürün kodu. Her bir ürün için eşsiz numara.
# Description: Ürün ismi
# Quantity: Ürün adedi. Faturalardaki ürünlerden kaçar tane satıldığını ifade etmektedir.
# InvoiceDate: Fatura tarihi ve zamanı.
# UnitPrice: Ürün fiyatı (Sterlin cinsinden)
# CustomerID: Eşsiz müşteri numarası
# Country: Ülke ismi. Müşterinin yaşadığı ülke.
###############################################################
# 2. Veriyi Anlama (Data Understanding)
###############################################################

import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
df_=pd.read_excel("datasets/online_retail_II.xlsx",sheet_name="Year 2009-2010")
df=df_.copy()
df.head()
df.shape
df.isnull().sum()
#eşsiz ürün sayısı nedir?
df["Description"].nunique()
df['Description'].value_counts().head()
df.groupby("Description").agg({"Quantity":"sum"}).head()
df.groupby("Description").agg({"Quantity":"sum"}).sort_values("Quantity",ascending=False)
df["Invoice"].nunique()
#Faturadaki toplam fiyatı bulma
df["New_Total_Price"]=df["Quantity"]*df["Price"]
df.groupby("Invoice").agg({"New_Total_Price":"sum"}).sort_values("New_Total_Price",ascending=False).head()
###############################################################
# 3. Veri Hazırlama (Data Preparation)
###############################################################

df.shape
df.isnull().sum()
df.dropna(inplace=True)
df.describe().T
#Invoice codunda C olanlar iptal edilen siparişler olduğundan onlardan kurtulalım
#Price ve quatitiydeki negatif değerler bunlardan kaynaklı olabilir
df=df[~(df["Invoice"].str.contains("C",na=False))]

###############################################################
# 4. RFM Metriklerinin Hesaplanması (Calculating RFM Metrics)
###############################################################
#Recency : Müşterinin son alışverişinden sonra geçen zaman
#Frequency : Müşterinin toplam  kaç kez alışveriş yaptığı
#Monetary : Müşterinin toplam bıraktığı para

#Veri seti 2009-2010 yılları arasında olduğundan analizi yaptığımız tarihi o tarihlere yakın seçmemiz mantıklı olur
df["InvoiceDate"].max()
today_date=dt.datetime(2010,12,11)
rfm=df.groupby("Customer ID").agg({"InvoiceDate": lambda date:(today_date-date.max()).days,
                                    "Invoice":lambda num: num.nunique(),
                                    "New_Total_Price":"sum"})
rfm.head()
rfm.columns=["Recency","Frequency","Monetary"]
rfm.describe().T
rfm=rfm[rfm["Monetary"]>0]
###############################################################
# 5. RFM Skorlarının Hesaplanması (Calculating RFM Scores)
###############################################################
rfm["Recency_Score"]=pd.qcut(rfm["Recency"],5,labels=[5,4,3,2,1])
rfm["Frequency_Score"]=pd.qcut(rfm["Frequency"].rank(method="first"),5,labels=[1,2,3,4,5])
rfm["Monetary_Score"]=pd.qcut(rfm["Monetary"],5,labels=[1,2,3,4,5])
rfm["RFM_score"]=rfm["Recency_Score"].astype(str)+rfm["Frequency_Score"].astype(str)
rfm.describe().T
#Önemli Müşteriler
rfm[rfm["RFM_score"]=="55"]
#Önemsiz Müşteriler
rfm[rfm["RFM_score"]=="11"]
###############################################################
# 6. RFM Segmentlerinin Oluşturulması ve Analiz Edilmesi (Creating & Analysing RFM Segments)
###############################################################
# regex
# RFM isimlendirmesi
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}
rfm["Segment"]=rfm["RFM_score"].replace(seg_map,regex=True)
rfm.groupby("Segment").agg({"Recency":"mean",
                            "Frequency":"mean",
                            "Monetary":["mean","count"]})
#Eğer bizden ilgiye ihtiyaç olanlar istenirse
rfm[rfm["Segment"]=="need_attention"]
#new customerların id si istenirse
rfm[rfm["Segment"]=="new_customers"].index
rfm_final=rfm["Segment"]
#bizden sonuçları csv dosyayı halinde isterler ise
#rfm_final.to_csv(rfm_final.csv)
###############################################################
# 7. Tüm Sürecin Fonksiyonlaştırılması
###############################################################
def create_rfm(dataframe, csv=False):

    # VERIYI HAZIRLAMA
    dataframe["TotalPrice"] = dataframe["Quantity"] * dataframe["Price"]
    dataframe.dropna(inplace=True)
    dataframe = dataframe[~dataframe["Invoice"].str.contains("C", na=False)]

    # RFM METRIKLERININ HESAPLANMASI
    today_date = dt.datetime(2011, 12, 11)
    rfm = dataframe.groupby('Customer ID').agg({'InvoiceDate': lambda date: (today_date - date.max()).days,
                                                'Invoice': lambda num: num.nunique(),
                                                "TotalPrice": lambda price: price.sum()})
    rfm.columns = ['recency', 'frequency', "monetary"]
    rfm = rfm[(rfm['monetary'] > 0)]

    # RFM SKORLARININ HESAPLANMASI
    rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

    # cltv_df skorları kategorik değere dönüştürülüp df'e eklendi
    rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) +
                        rfm['frequency_score'].astype(str))


    # SEGMENTLERIN ISIMLENDIRILMESI
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }

    rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)
    rfm = rfm[["recency", "frequency", "monetary", "segment"]]
    rfm.index = rfm.index.astype(int)

    if csv:
        rfm.to_csv("rfm.csv")

    return rfm

df = df_.copy()

rfm_new = create_rfm(df)







