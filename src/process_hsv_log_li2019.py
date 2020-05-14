
import glob
import os
import pandas as pd
import os
import glob
from pyproj import Geod
from pyproj import Proj
import numpy as np

sourcepath = 'S:/HSV/Log Files/clean/'
outputpath = 'S:/LI2019_V05/HSV/DATA/LOG/'

#convert_wgs_to_utm function, see https://stackoverflow.com/questions/40132542/get-a-cartesian-projection-accurate-around-a-lat-lng-pair

def convert_wgs_to_utm(lon, lat):
    utm_band = str((np.floor((lon + 180) / 6 ) % 60) + 1)
    if len(utm_band) == 1:
        utm_band = '0'+utm_band
    if lat >= 0:
        epsg_code = '326' + utm_band
    else:
        epsg_code = '327' + utm_band
    return epsg_code

def process_postion(item):
    
    try:
        lat = float(item['data']['values']['latitude'])
        lon = float(item['data']['values']['longitude'])
    except:
        lon = np.nan
        lat = np.nan

    return {'timestamp':item.name,'latitude':lat,'longitude':lon}

def process_pressure(item):
    return {'timestamp':item.name,"pressure":float(item['data']['values']['pressure'])}
    
def process_usbl(item):
    return {'timestamp':item.name,"X":item['data']['values']['usbl_athwart'],
           "Y":item['data']['values']['usbl_fore_aft'],
           "Depth":item['data']['values']['usbl_depth']}

    
def process_alt(item):
    
    try:
        alt = float(item['data']['values']['altitude'])
    except:
        alt=np.nan
    if alt==99.99:
        alt=np.nan

    return {'timestamp':item.name,"Altitude":alt}


def process_heading(item):
    try:
        heading = float(item['data']['values']['heading'])
    except:
        heading=pd.np.nan
    return {'timestamp':item.name,"ShipHeading":heading} 

def removespikes(data,threshold,window=5):
    difference = np.abs(data -data.rolling(window=5, center=True).median())
    outlier_idx = difference > threshold
    data[outlier_idx]=np.nan
    return data

def get_median_filtered(signal, threshold=3):
    signal = signal.copy()
    difference = np.abs(signal - np.median(signal))
    median_difference = np.median(difference)
    if median_difference == 0:
        s = 0
    else:
        s = difference / float(median_difference)
    mask = s > threshold
    signal[mask] = np.median(signal)
    return signal



def process_file(file):
    log =pd.read_json(file,lines=True).set_index('timestamp')
    sonardyne ='.*X:(?P<X>[0-9-]*\.[0-9]*).*Y:(?P<Y>[0-9-]*\.[0-9]*).*D:(?P<D>[0-9-]*\.[0-9]*).*H:(?P<H>[0-9-]*\.[0-9]*)'
    sondardyne_raw=log[log.message=="linnaeus.sonardyne_scout.usbl.raw"]['data'].str.extract(sonardyne)[['X','Y','D','H']].astype(float).dropna()

    hsv_position  = pd.DataFrame.from_records(log[(log.message=="hsv.application.position.eng") | ( log.message=="hsv.sonardyne_scout.calculated_beacon_position.eng")].apply(process_postion,axis=1)).set_index('timestamp')
    hsv_pressure  = pd.DataFrame.from_records(log[log.message=="hsv.pressure.telemetry.eng"].apply(process_pressure,axis=1)).set_index('timestamp')
    ship_position = pd.DataFrame.from_records(log[log.message=="linnaeus.furuno.gps.eng"].apply(process_postion,axis=1)).set_index('timestamp')
    hsv_altitude  = pd.DataFrame.from_records(log[log.message=="hsv.benthos.altimeter.eng"].apply(process_alt,axis=1)).set_index('timestamp')
    hsv_altitude = hsv_altitude[hsv_altitude.Altitude<20]
    ship_heading  = pd.DataFrame.from_records(log[log.message=="linnaeus.furuno.heading.eng"].apply(process_heading,axis=1)).set_index('timestamp')
    usbl_position  = pd.DataFrame.from_records(log[log.message=="linnaeus.sonardyne_scout.usbl.eng"].apply(process_usbl,axis=1)).set_index('timestamp')
    usbl_position.Depth =usbl_position.Depth.str.extract('.*:(?P<value>.*)')['value'].astype(float)
    usbl_position.X =usbl_position.X.str.extract('.*:(?P<value>.*)')['value'].astype(float)
    usbl_position.Y =usbl_position.Y.str.extract('.*:(?P<value>.*)')['value'].astype(float)
    usbl_position.index =usbl_position.index.round('100L')
    usbl_position.dropna(inplace=True)

    hsv_altitude.index =hsv_altitude.index.round('100L')

    hsv_position.columns = ['HSVLatitude','HSVLongitude']
    hsv_position.index =hsv_position.index.round('100L')
    hsv_position.columns = ['HSVLatitude','HSVLongitude']
    hsv_position.index =hsv_position.index.round('100L')
    hsv_pressure.columns = ['HSVPressure']
    hsv_pressure.index =hsv_pressure.index.round('100L')
    hsv_altitude.columns = ['HSVAltitude']
    hsv_altitude.index =hsv_altitude.index.round('100L')
    ship_position.columns = ['ShipLatitude','ShipLongitude']
    ship_position.index =ship_position.index.round('100L')
    utmcode =convert_wgs_to_utm(ship_position.ShipLongitude[0],ship_position.ShipLatitude[0])
    utmproj =Proj(init='epsg:{0:1.5}'.format(utmcode))
    ship_position.dropna(inplace=True)
    e,n =utmproj(ship_position.ShipLongitude.values,ship_position.ShipLatitude.values)
    ship_position['ShipEasting']=e
    ship_position['ShipNorthing']=n
    hsv_position.dropna(inplace=True)
    e,n =utmproj(hsv_position.HSVLongitude.values,hsv_position.HSVLatitude.values)
    hsv_position
    hsv_position['HSVEasting']=e
    hsv_position['HSVNorthing']=n

    ship_heading['u'] = np.cos(np.deg2rad(ship_heading.ShipHeading)).rolling(7,min_periods=1,center=True).mean()
    ship_heading['v'] = np.sin(np.deg2rad(ship_heading.ShipHeading)).rolling(7,min_periods=1,center=True).mean()
    ship_heading['ShipHeadingSmoothed'] =np.rad2deg(np.arctan2(ship_heading['v'],ship_heading['u']))


    ship_heading.loc[ ship_heading.ShipHeadingSmoothed<0,'ShipHeadingSmoothed'] = 360+ship_heading.loc[ ship_heading.ShipHeadingSmoothed<0,'ShipHeadingSmoothed']
    position_data = pd.concat([sondardyne_raw,ship_position,ship_heading,hsv_position,hsv_altitude,hsv_pressure],sort=False).sort_index()

    position_data =position_data[['ShipLatitude','ShipLongitude','ShipEasting','ShipNorthing','ShipHeading','HSVEasting','HSVNorthing','ShipHeadingSmoothed','HSVAltitude','HSVPressure']].interpolate().dropna()
    sondardyne_raw =sondardyne_raw.join(position_data).dropna()
    cangle = np.exp((2*np.pi*(sondardyne_raw.ShipHeadingSmoothed)/360)*1j)
    x =cangle * (sondardyne_raw.Y.values+sondardyne_raw.X.values * 1j)
    sondardyne_raw['XPrime']=x.values.real
    sondardyne_raw['YPrime']=x.values.imag
    sondardyne_raw.XPrime = removespikes(sondardyne_raw.XPrime,20).interpolate().rolling(7,min_periods=1,center=True).mean()
    sondardyne_raw.YPrime = removespikes(sondardyne_raw.YPrime,20).interpolate().rolling(7,min_periods=1,center=True).mean()
    sondardyne_raw['UsblNorthing'] =sondardyne_raw['ShipNorthing']+sondardyne_raw['XPrime']
    sondardyne_raw['UsblEasting'] =sondardyne_raw['ShipEasting']+sondardyne_raw['YPrime']
    sondardyne_raw['HSVDistance']=np.power(np.power(sondardyne_raw.HSVEasting - sondardyne_raw.ShipEasting,2) + np.power(sondardyne_raw.HSVNorthing - sondardyne_raw.ShipNorthing,2),0.5)
    sondardyne_raw['RawDistance']=np.power(np.power(sondardyne_raw.X,2) + np.power(sondardyne_raw.Y,2),0.5)
    sondardyne_raw['UsblDistance']=np.power(np.power(sondardyne_raw.UsblEasting - sondardyne_raw.ShipEasting,2) + np.power(sondardyne_raw.UsblNorthing - sondardyne_raw.ShipNorthing,2),0.5)
    sondardyne_raw['UtmCode'] = 'epsg:{0:1.5}'.format(utmcode)
    return sondardyne_raw


def task_convert_json_log():
    """ convert HSV json logs to csv
    """
    def action(dependencies,targets):
        """

        :param dependencies: list of files to process
        :param targets: list of file to output
        :return:
        """
        data = process_file(list(dependencies)[0])
        data.to_csv(list(targets)[0],index=True)
    
    os.makedirs(outputpath,exist_ok=True)
    files = glob.glob('s:/HSV/Log Files/clean/*.json')
    for file in files:
        print(file)
        basename = os.path.splitext(os.path.basename(file))[0]
        yield {
            'name': str(file),
            'actions': [(action, [], )],
            # 'task' keyword arg is added automatically
            'targets': [outputpath+basename.replace('.json','.csv')],
            'file_dep': [file],
            'uptodate': [True, ],
            'clean': True,
        }

if __name__ == '__main__':
    import doit

    #print(globals())
    doit.run(globals())