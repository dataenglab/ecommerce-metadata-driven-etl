import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle
from matplotlib import font_manager
names={f.name for f in font_manager.fontManager.ttflist}
plt.rcParams['font.family']='Arial' if 'Arial' in names else ('Liberation Sans' if 'Liberation Sans' in names else 'DejaVu Sans')
MONO='Liberation Mono' if 'Liberation Mono' in names else 'DejaVu Sans Mono'
CYAN='#E6F8FC'; GREY='#F3F3F3'
W,H=18.2,18.6
fig,ax=plt.subplots(figsize=(W/2.2,H/2.2),dpi=300)
ax.set_xlim(0,W);ax.set_ylim(0,H);ax.axis('off')
R=0.34; HD=0.52; TW=5.0
def table(x,top,name,cols,fill=CYAN,schema='dwh'):
    h=HD+R*len(cols)+0.12
    ax.add_patch(FancyBboxPatch((x,top-h),TW,h,boxstyle='round,pad=0,rounding_size=0.12',fc='white',ec='black',lw=1.0,zorder=2))
    ax.add_patch(Rectangle((x,top-HD),TW,HD,fc=fill,ec='black',lw=1.0,zorder=3))
    ax.text(x+TW/2,top-HD/2,f'{schema}.{name}',ha='center',va='center',fontsize=6.9,fontweight='bold',family=MONO,zorder=4)
    ys={}
    for k,(key,col,typ) in enumerate(cols):
        y=top-HD-0.06-R*(k+0.5); ys[col]=y
        ax.text(x+0.12,y,key,ha='left',va='center',fontsize=5.9,fontweight='bold',color='#1F4E79',zorder=4)
        ax.text(x+0.58,y,col,ha='left',va='center',fontsize=5.9,family=MONO,zorder=4,fontweight='bold' if key else 'normal')
        ax.text(x+TW-0.12,y,typ,ha='right',va='center',fontsize=5.3,color='#555555',zorder=4)
    return {'x0':x,'x1':x+TW,'top':top,'bot':top-h,'y':ys}
def edge(x_from,y_from,x_to,y_to,xm,dashed=False):
    ls=(0,(3,2)) if dashed else '-'
    ax.plot([x_from,xm,xm,x_to],[y_from,y_from,y_to,y_to],color='black',lw=0.8,ls=ls,zorder=1)
    # cardinality labels
    d=0.16 if x_to>x_from else -0.16
    ax.text(x_from+d*1.2,y_from+0.11,'N',fontsize=5.6,ha='center',va='bottom',zorder=5)
    ax.text(x_to-d*1.2,y_to+0.11,'1',fontsize=5.6,ha='center',va='bottom',zorder=5)
ax.text(0.2,H-0.35,'Three fact tables share the date, customer and product dimensions',fontsize=8.6,fontweight='bold',va='top')
xL,xC,xR=0.2,6.65,13.1
TOP=H-1.15
ol=table(xC,TOP,'fact_order_line',[('PK','order_id','TEXT'),('PK','line_number','SMALLINT'),('FK','date_key','INTEGER'),('FK','customer_sk','BIGINT'),('FK','product_sk','BIGINT'),('','order_ts_utc','TIMESTAMPTZ'),('','quantity','INTEGER'),('','unit_price','NUMERIC'),('','discount_amount','NUMERIC'),('','shipping_amount','NUMERIC'),('','total_amount','NUMERIC'),('','currency_code','CHAR(3)'),('','run_id','UUID')])
cs=table(xC,ol['bot']-0.55,'fact_clickstream_event',[('PK','event_id','TEXT'),('','session_id','TEXT'),('FK','date_key','INTEGER'),('FK','customer_sk','BIGINT, nullable'),('FK','product_sk','BIGINT, nullable'),('','event_ts_utc','TIMESTAMPTZ'),('','event_type','TEXT'),('','page_url','TEXT'),('','quantity','INTEGER'),('','run_id','UUID')])
pay=table(xC,cs['bot']-0.55,'fact_payment',[('PK','payment_id','TEXT'),('','order_id','TEXT, no FK'),('FK','date_key','INTEGER'),('','created_at_utc','TIMESTAMPTZ'),('','gateway','TEXT'),('','transaction_type','TEXT'),('','payment_status','TEXT'),('','payment_method','TEXT'),('','sales_channel','TEXT'),('','amount_minor','BIGINT'),('','currency_code','CHAR(3)'),('','run_id','UUID')])
dd=table(xL,TOP,'dim_date',[('PK','date_key','INTEGER'),('','full_date','DATE'),('','year','SMALLINT'),('','quarter','SMALLINT'),('','month','SMALLINT'),('','iso_week','SMALLINT'),('','day_of_week','SMALLINT'),('','is_weekend','BOOLEAN')])
cut=cs['y']['customer_sk']+HD+0.06+R*0.5
cu=table(xL,cut,'dim_customer',[('PK','customer_sk','BIGINT'),('','customer_hash','CHAR(64)'),('','email_hash','CHAR(64)'),('','ship_country *','CHAR(2)'),('','ship_postcode_prefix *','VARCHAR(4)'),('','valid_from','TIMESTAMPTZ'),('','valid_to','TIMESTAMPTZ'),('','is_current','BOOLEAN')])
pr=table(xR,TOP,'dim_product',[('PK','product_sk','BIGINT'),('','product_sku','TEXT'),('','product_name *','TEXT'),('','category *','TEXT'),('','valid_from','TIMESTAMPTZ'),('','valid_to','TIMESTAMPTZ'),('','is_current','BOOLEAN')])
def bus(xm,ends,dim_x,dim_y,fact_side):
    ys=[y for _,y in ends]+[dim_y]
    ax.plot([xm,xm],[min(ys),max(ys)],color='black',lw=0.8,zorder=1)
    for fx,fy in ends:
        ax.plot([fx,xm],[fy,fy],color='black',lw=0.8,zorder=1)
        ax.text(fx+(0.15 if fact_side=='right' else -0.15),fy+0.09,'N',fontsize=5.6,ha='center',va='bottom',zorder=5)
        ax.plot([xm],[fy],marker='o',ms=1.8,color='black',zorder=2)
    ax.plot([xm,dim_x],[dim_y,dim_y],color='black',lw=0.8,zorder=1)
    ax.text(dim_x+(0.15 if dim_x<xm else -0.15),dim_y+0.09,'1',fontsize=5.6,ha='center',va='bottom',zorder=5)
bus(5.5,[(ol['x0'],ol['y']['date_key']),(cs['x0'],cs['y']['date_key']),(pay['x0'],pay['y']['date_key'])],dd['x1'],dd['y']['date_key'],'left')
bus(6.1,[(ol['x0'],ol['y']['customer_sk']),(cs['x0'],cs['y']['customer_sk'])],cu['x1'],cu['y']['customer_sk'],'left')
bus(12.4,[(ol['x1'],ol['y']['product_sk']),(cs['x1'],cs['y']['product_sk'])],pr['x0'],pr['y']['product_sk'],'right')
# etl schema (control tables)
et_top=cs['bot']-0.1
lg=table(xR,et_top,'pipeline_logs',[('PK','log_id','BIGINT'),('','run_id','UUID'),('','source_file','TEXT'),('','status','TEXT'),('','rows_read','INTEGER'),('','rows_inserted','INTEGER'),('','rows_quarantined','INTEGER')],fill=GREY,schema='etl')
qs=table(xR,lg['bot']-0.55,'quarantine_staging',[('PK','quarantine_id','BIGINT'),('FK','log_id','BIGINT'),('','run_id','UUID'),('','source_row_number','INTEGER'),('','error_code','TEXT'),('','raw_record','JSONB')],fill=GREY,schema='etl')
ax.plot([qs['x0'],qs['x0']-0.3,qs['x0']-0.3,lg['x0']],[qs['y']['log_id'],qs['y']['log_id'],lg['y']['log_id'],lg['y']['log_id']],color='black',lw=0.8,zorder=1)
ax.text(qs['x0']-0.15,qs['y']['log_id']+0.09,'N',fontsize=5.6,ha='center',va='bottom',zorder=5); ax.text(lg['x0']-0.15,lg['y']['log_id']+0.09,'1',fontsize=5.6,ha='center',va='bottom',zorder=5)
# legend
ly=0.75
ax.text(0.2,ly+0.35,'PK primary key · FK foreign key · 1–N one dimension row to many fact rows · * attribute tracked as SCD Type 2 (new row version on change)',fontsize=6.0,va='center')
ax.text(0.2,ly,'fact_payment.order_id refers to orders logically but has no foreign key; run_id in every fact row identifies the load run recorded in etl.pipeline_logs.',fontsize=6.0,va='center',color='#333333')
ax.text(0.2,ly-0.35,'All timestamps are stored in UTC (TIMESTAMPTZ, columns ending in _utc); date_key is derived from the UTC date.',fontsize=6.0,va='center',color='#333333')
from pathlib import Path
fig.savefig(Path(__file__).resolve().parent / 'star_schema.png',bbox_inches='tight',pad_inches=0.06,facecolor='white')

