#%%
from datetime import date
import pandas as pd
import holidays
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

def get_national_holidays(start_date,end_date,country,sdiv,holiday_type,fin_market):
    """Return a list of the dates of national holidays between two dates 
    for a given country.
    
    Args:
        start_date (str): Start date of the period
        end_date (str): End date of the period
        country (str): Country to get the national holidays for
        sdiv (str): Subdivision Country (States, Provinces, etc...)
    
    Returns:
        list: List of national holiday dates
    """

    # Get the Bank Holidays for the given country
    if holiday_type == 'Country':
        holiday_days = holidays.CountryHoliday(country, subdiv = sdiv)
    elif holiday_type == 'Financial':
        holiday_days = holidays.financial_holidays(fin_market)

    # Create a list of dates between the start and end date
    date_range = pd.date_range(start_date, end_date)

    # Filter the dates to only include Bank Holidays
    national_holidays = [date for date in date_range if date in holiday_days]

    return national_holidays

def create_calendar(country,sdiv,holiday_type,fin_market):
    """Return a calendar dataframe
    
    Args:
        country (str): Country to get the national holidays for
        sdiv (str): Subdivision Country (States, Provinces, etc...)
        holiday_type (str): Financial or Country
        fin_market (str): Financial market name
    
    Returns:
        MY_DATE: date of the year
        IS_HOLIDAY: flag True or False
        NEXT_WORKING_DAY: next working day considering holidays
        CODID: MY_DATE as integer
    """
    # Get Start and End date
    start_date = date(date.today().year-3, 1, 1)
    end_date = date(date.today().year+3, 12, 31)

    # Get list of holidays
    list_holidays = get_national_holidays(start_date,end_date,country,sdiv,holiday_type,fin_market)

    # Transform list into Pandas dataframe
    df_list_holidays = pd.DataFrame (list_holidays, columns = ['HOLIDAY'])
    df_list_holidays.index = list(df_list_holidays["HOLIDAY"])

    # Create a range of dates for calendar
    date_range = pd.date_range(start_date, end_date)
    df_calendar = date_range.to_frame(name='MY_DATE')

    # Join calendar with holiday to define the flag IS_BANK_HOLIDAY
    df_calendar_holiday = df_calendar.join(df_list_holidays)
    df_calendar_holiday["IS_BANK_HOLIDAY"] = df_calendar_holiday['HOLIDAY'].apply(lambda x: False if pd.isnull(x) else True)

    # Add column NEXT_WORKING_DAY and COBID
    df_calendar_holiday["NEXT_BUSINESS_DAY"] = df_calendar['MY_DATE'] + pd.offsets.CustomBusinessDay(1, holidays=list_holidays)
    df_calendar_holiday["COBID"] = df_calendar['MY_DATE'].apply(lambda x: int(x.strftime("%Y%m%d")))
    
    # Drop column HOLIDAY
    df_calendar_holiday.drop('HOLIDAY', axis=1, inplace=True)
    
    return df_calendar_holiday
