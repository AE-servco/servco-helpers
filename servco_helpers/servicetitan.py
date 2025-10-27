from zoneinfo import ZoneInfo
import servicepytan as sp
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo
import pandas as pd
from google.cloud import secretmanager

def get_last_midnight_aest_in_utc():
    now_aest = datetime.now(ZoneInfo("Australia/Sydney"))
    last_midnight_aest = now_aest.replace(hour=0, minute=0, second=0, microsecond=0)
    last_midnight_utc = last_midnight_aest.astimezone(ZoneInfo("UTC"))
    return last_midnight_utc.isoformat().replace("+00:00", "Z")

def get_secret(secret_id, project_id="servco1", version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    return secret_payload

def extract_relevant_data(response_data, attrs):
    def change_and_strip_timezone(date_in, timezone="Australia/Sydney"):
        return datetime.fromisoformat(date_in).astimezone(ZoneInfo(timezone)).replace(tzinfo=None)

    def extract_attr(attr_tuple):
        dtype_defaults = {
            'int': -1,
            'str': 'no_data',
            'float': -1.0,
            'datetime': datetime(1970,1,1,0,0,0),
            'date': date(1970,1,1),
            'list': [],
            'bool': False
        }
        dtype = attr_tuple[-1]
        attr = attr_tuple[:-1]

        tmp_data=response_data.get(attr[0])
        if tmp_data:
            for i in range(1,len(attr)):
                if tmp_data.get(attr[i]):
                    tmp_data = tmp_data.get(attr[i])
                else:
                    return dtype_defaults[dtype]
            if dtype == 'datetime':
                if datetime.fromisoformat(tmp_data).date() == date(1,1,1):
                    return dtype_defaults[dtype]
                return change_and_strip_timezone(tmp_data)
            return tmp_data
        else:
            return dtype_defaults[dtype]

    output = []

    for attr in attrs:
        output.append(extract_attr(attr))

    return output

def convert_attr_tuples(attrs, data_name='', extra_cols = [('state','str')]):
    '''
    Converts list of attr tuples used in extract_relevant_data() to dtypes dictionary for use in pandas later. data_name is the alias for the ST data coming in (job, call, customer, etc.)
    Params:
        attrs = list(tuples) e.g. [('id', 'int'), ('jobNumber', 'int')]
        data_name = str
    '''
    dtype_map = {
        'int': 'int64',
        'float': 'float64',
        'str': 'str',
        'datetime': 'datetime64[ns]',
        'date': 'datetime64[ns]',
        'bool': 'bool',
        'list': 'object',
    }
    attrs_copy = attrs.copy()

    # if attrs_copy[0][0] == 'id':
    #     attrs_copy[0] = (f'{data_name}_STid', attrs_copy[0][1])
    attrs_copy = extra_cols + attrs_copy
    attr_dtypes = {
        '_'.join(attrs_copy[:-1]): dtype_map[attrs_copy[-1]] for attrs_copy in attrs_copy
    }
    return attr_dtypes

def handle_call_data(call_response_data_ls):
    call_attrs = [
        ('leadCall', 'duration', 'str'),
        ('leadCall', 'callType', 'str'),
        ('leadCall', 'reason','id', 'int'),
        ('leadCall', 'reason', 'lead', 'bool'),
        ('leadCall', 'reason', 'name', 'str'),
    ]

    calls_dtypes = convert_attr_tuples(call_attrs, 'call',extra_cols=[('state','str')])
    call_data_ls = []
    for call in call_response_data_ls:
        call_data_ls.append(extract_relevant_data(call, call_attrs))
    calls_df = pd.DataFrame(call_data_ls, columns=calls_dtypes)


    calls_df['duration_td'] = calls_df['leadCall_duration'].apply(lambda x: pd.to_timedelta(x))

    total_calls = calls_df.shape[0]
    lead_calls = calls_df[
        ((calls_df['leadCall_callType'] != 'Excused') & (calls_df['leadCall_callType'] != 'NotLead') & (calls_df['duration_td'] > timedelta(seconds=59))) | 
        (calls_df['leadCall_callType'] == 'Booked') | 
        ((calls_df['leadCall_reason_lead'] == True) & (calls_df['leadCall_callType'] == 'Unbooked'))
        ].shape[0]
    abandoned = calls_df[calls_df['leadCall_callType'] == 'Abandoned'].shape[0]
    inbound_booked = calls_df[calls_df['leadCall_callType'] == 'Booked'].shape[0]
    unbooked_unachievable = calls_df[(calls_df['leadCall_callType'] == 'Unbooked') & ((calls_df['leadCall_reason_id'] != 28) & (calls_df['leadCall_reason_id'] != 29))].shape[0]
    plumber_unavailable_calls = calls_df[(calls_df['leadCall_reason_name'] == "No Plumber Availability")].shape[0]
    outside_service_area = calls_df[(calls_df['leadCall_reason_name'] == "Outside of Service Area")].shape[0]
    service_not_provided = calls_df[(calls_df['leadCall_reason_name'] == "Service Not Offered")].shape[0]

    output = {
        'total_calls': total_calls,
        'lead_calls': lead_calls,
        'inbound_booked': inbound_booked,
        'unbooked_unachievable': unbooked_unachievable,
        'abandoned': abandoned,
        'plumber_unavailable_calls': plumber_unavailable_calls,
        'outside_service_area': outside_service_area,
        'service_not_provided': service_not_provided,
    }

    return output

# Jobs data handling


def get_job_type_thresholds(job_type_response_data_ls):

    job_type_thresholds = {}
    for job in job_type_response_data_ls:
        job_type_thresholds[job.get('id')] = job.get('soldThreshold')
    
    return job_type_thresholds

# input = list of all calls from API
def handle_job_data(job_response_data_ls):
    job_attrs = [
        ('leadCallId', 'int'),
        ('bookingId', 'int'),
    ]

    jobs_dtypes = convert_attr_tuples(job_attrs, 'job')
    job_data_ls = []
    for job in job_response_data_ls:
        job_data_ls.append(extract_relevant_data(job, job_attrs))
    jobs_df = pd.DataFrame(job_data_ls, columns=jobs_dtypes)

    manual_booked = jobs_df[(jobs_df['leadCallId'] == -1) & (jobs_df['bookingId'] == -1)].shape[0]
    
    output = {
        'manual_booked': manual_booked,
    }

    return output

# input = list of all calls from API
def handle_job_completed_data(job_completed_response_data_ls, job_type_sold_thresholds=None):
    if not job_type_sold_thresholds:
        print("Give me job_type_sold_thresholds!!!")
        return
    job_attrs = [
        ('jobStatus', 'str'),
        ('jobTypeId', 'int'),
        ('noCharge', 'bool'),
        ('leadCallId', 'int'),
        ('bookingId', 'int'),
        ('total', 'float'),
    ]

    jobs_dtypes = convert_attr_tuples(job_attrs, 'job')
    job_data_ls = []
    for job in job_completed_response_data_ls:
        job_data_ls.append(extract_relevant_data(job, job_attrs))
    jobs_df = pd.DataFrame(job_data_ls, columns=jobs_dtypes)
    jobs_df['sold_thresh'] = jobs_df['jobTypeId'].apply(lambda x: job_type_sold_thresholds[x])

    completed_income_jobs = jobs_df[(jobs_df['jobStatus'] == 'Completed') & (jobs_df['total'] > 0)]
    completed_income_jobs_size = completed_income_jobs.shape[0]
    completed_jobs_total_income = completed_income_jobs['total'].sum() / 1.1 # total includes GST
    opportunities_booked = jobs_df[
        (jobs_df['jobStatus'] == 'Completed') &
        ((jobs_df['noCharge'] == False) | (jobs_df['total'] >= jobs_df['sold_thresh']))
        ].shape[0]
    booked_converted = jobs_df[
        (jobs_df['jobStatus'] == 'Completed') &
        (jobs_df['total'] >= jobs_df['sold_thresh'])
        ].shape[0]

    output = {
        'completed_income_jobs': completed_income_jobs_size,
        'estimated_revenue': completed_jobs_total_income,
        'opportunities_booked': opportunities_booked,
        'booked_converted': booked_converted,
    }

    return output

# input = list of all calls from API
def handle_payments_data(payments_response_data_ls):
    payment_attrs = [
        ('total', 'str'),
    ]

    payment_dtypes = convert_attr_tuples(payment_attrs, 'payment')
    payment_data_ls = []
    for payment in payments_response_data_ls:
        payment_data_ls.append(extract_relevant_data(payment, payment_attrs))
    payment_df = pd.DataFrame(payment_data_ls, columns=payment_dtypes)
    payment_df['total'] = payment_df['total'].astype(float)

    total_income = payment_df['total'].sum()

    output = {
        'total_income': total_income,
    }

    return output

# Booking data handling

# input = list of all calls from API
def handle_booking_data(booking_response_data_ls):
    booking_attrs = [
        ('status', 'str'),
        ('dismissingReasonId', 'int'),
        ('bookingProviderId', 'int'),
    ]

    bookings_dtypes = convert_attr_tuples(booking_attrs, 'booking')

    booking_data_ls = []
    for booking in booking_response_data_ls:
        booking_data_ls.append(extract_relevant_data(booking, booking_attrs))

    bookings_df = pd.DataFrame(booking_data_ls, columns=bookings_dtypes)

    online_bookings_converted = bookings_df[(bookings_df['bookingProviderId'] != -1) & (bookings_df['status'] == 'Converted')].shape[0]
    online_bookings_dismissed_lead_unachievable = bookings_df[
        (bookings_df['dismissingReasonId'] == 32) |
        (bookings_df['dismissingReasonId'] == 70968574) | 
        (bookings_df['dismissingReasonId'] == 142829723)].shape[0]
    online_bookings_dismissed_lead_achievable = bookings_df[
        (bookings_df['dismissingReasonId'] == 28) |
        (bookings_df['dismissingReasonId'] == 29)].shape[0]

    output = {
        'online_bookings_dismissed_lead_unachievable': online_bookings_dismissed_lead_unachievable,
        'online_bookings_converted': online_bookings_converted,
        'online_bookings_dismissed_lead_achievable': online_bookings_dismissed_lead_achievable,
    }

    return output

# Sold estimates data handling

# input = list of all calls from API
def handle_sold_estimates_data(sold_estimates_response_data_ls):
    sold_estimates_attrs = [
        ('subtotal', 'float'),
    ]

    sold_estimates_dtypes = convert_attr_tuples(sold_estimates_attrs, 'soldestimates')

    sold_estimates_data_ls = []
    for sold_estimate in sold_estimates_response_data_ls:
        sold_estimates_data_ls.append(extract_relevant_data(sold_estimate, sold_estimates_attrs))

    sold_estimates_df = pd.DataFrame(sold_estimates_data_ls, columns=sold_estimates_dtypes)

    sales = round(sold_estimates_df['subtotal'].sum(), 2)

    output = {
        'sales': sales,
    }

    return output

def collate_data(data_dicts):
    final_data = dict()
    for d in data_dicts:
        final_data.update(d)

    if 'inbound_booked' in final_data and 'manual_booked' in final_data and 'online_bookings_converted' in final_data:
        final_data['total_booked'] = final_data['inbound_booked'] + final_data['manual_booked'] + final_data['online_bookings_converted']
    if 'lead_calls' in final_data and 'manual_booked' in final_data and 'online_bookings_converted' in final_data and 'online_bookings_dismissed_lead_achievable' in final_data:    
        final_data['leads_total'] = final_data['lead_calls'] + final_data['manual_booked'] + final_data['online_bookings_converted'] + final_data['online_bookings_dismissed_lead_achievable']
    if 'total_booked' in final_data and 'leads_total' in final_data:
        final_data['booking_rate'] = final_data['total_booked'] / final_data['leads_total'] if final_data['leads_total'] != 0 else 0

    return final_data

def build_API_call_filter(cols_wanted):
    cols_wanted_set = set(cols_wanted)

    call_cols = {
        'total_calls',
        'lead_calls',
        'inbound_booked',
        'unbooked_unachievable',
        'abandoned',
        'leads_total',
        'total_booked',
        'plumber_unavailable_calls',
        'outside_service_area',
        'service_not_provided',
    }
    jobs_created_cols = {
        'manual_booked',
        'leads_total',
        'total_booked',
    }
    jobs_completed_cols = {
        'completed_income_jobs',
        'estimated_revenue',
        'opportunities_booked',
        'booked_converted',
        'opportunity_conversion_rate',
    }
    booking_cols = {
        'online_bookings_dismissed_lead_unachievable',
        'online_bookings_converted',
        'online_bookings_dismissed_lead_achievable',
        'leads_total',
        'total_booked',
    }
    payment_cols = {
        'total_income',
    }
    sold_estimate_cols = {
        'sales',
    }

    api_requests_to_call = set()

    if not cols_wanted_set.isdisjoint(call_cols):
        api_requests_to_call.add('calls')
    if not cols_wanted_set.isdisjoint(jobs_created_cols):
        api_requests_to_call.add('jobs_created')
    if not cols_wanted_set.isdisjoint(jobs_completed_cols):
        api_requests_to_call.add('jobs_completed')
    if not cols_wanted_set.isdisjoint(booking_cols):
        api_requests_to_call.add('bookings')
    if not cols_wanted_set.isdisjoint(payment_cols):
        api_requests_to_call.add('payments')
    if not cols_wanted_set.isdisjoint(sold_estimate_cols):
        api_requests_to_call.add('sold_estimates')

    return api_requests_to_call

def state_codes():
    codes = {
        'NSW_old': 'alphabravo',
        'VIC_old': 'victortango',
        'QLD_old': 'echozulu',
        'NSW': 'foxtrotwhiskey',
        'WA': 'sierradelta',
        'QLD': 'bravogolf',
    }
    return codes

def get_new_data(state, cols_wanted, date=None, st_data_service=None):

    state_code = state_codes()[state]

    if not st_data_service:
        st_conn = sp.auth.servicepytan_connect(app_key=get_secret("ST_app_key_tester"), tenant_id=get_secret(f"ST_tenant_id_{state_code}"), client_id=get_secret(f"ST_client_id_{state_code}"), 
        client_secret=get_secret(f"ST_client_secret_{state_code}"), timezone="Australia/Sydney")
        st_data_service = sp.DataService(conn=st_conn)

    midnight_today_aest = datetime.now(ZoneInfo("Australia/Sydney")).replace(hour=0, minute=0, second=0).replace(tzinfo=None)
    now = datetime.now(ZoneInfo("Australia/Sydney")).replace(tzinfo=None)

    end_datetime = now.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")

    apis_to_call = build_API_call_filter(cols_wanted)

    handled_data = []

    if not date:
        start_time = midnight_today_aest
        end_time = now
    else:
        start_time = datetime.combine(date, time(0,0,0))
        end_time = datetime.combine(date, time(23,59,59))

    # ============================== Calls ==============================

    if 'calls' in apis_to_call:
        call_response_data_ls = st_data_service.get_calls_between(start_time, end_time)
        handled_data.append(handle_call_data(call_response_data_ls))

    # ============================== Jobs (Created) ==============================

    if 'jobs_created' in apis_to_call:
        job_response_data_ls = st_data_service.get_jobs_created_between(start_time, end_time)
        handled_data.append(handle_job_data(job_response_data_ls))

    # ============================== Jobs (Completed) ==============================

    if 'jobs_completed' in apis_to_call:
        job_type_data_thresholds = {}

        job_types = st_data_service.get_job_types()
        for entry in job_types:
            job_type_data_thresholds[entry.get('id')] = entry.get('soldThreshold')

        job_completed_response_data_ls = st_data_service.get_jobs_completed_between(start_time, end_time, job_status=["Completed"])
        handled_data.append(handle_job_completed_data(job_completed_response_data_ls, job_type_data_thresholds))

    # ============================== Bookings ==============================

    if 'bookings' in apis_to_call:
        booking_response_data_ls = st_data_service.get_bookings_between(start_time, end_time)
        handled_data.append(handle_booking_data(booking_response_data_ls))

    # ============================== Payments ==============================

    if 'payments' in apis_to_call:
        payments_response_data_ls = st_data_service.get_payments_between(start_time, end_time)
        handled_data.append(handle_payments_data(payments_response_data_ls))
    
    # ============================== Sold Estimates ==============================

    if 'sold_estimates' in apis_to_call:
        sold_estimates_response_data_ls = st_data_service.get_sold_estimates_between(start_time, end_time)
        handled_data.append(handle_sold_estimates_data(sold_estimates_response_data_ls))

    # ============================== Final stuff ==============================

    daily_call_data = collate_data(handled_data)

    if 'estimated_revenue' in daily_call_data:
        daily_call_data['estimated_revenue'] = round(daily_call_data['estimated_revenue'],2)
    if date:
        daily_call_data['date'] = date.strftime('%Y-%m-%d')
    else:  
        daily_call_data['end_time_utc'] = end_datetime

    output_data = {k: daily_call_data[k] for k in cols_wanted}

    return output_data

def add_aux_data(data):
    if 'estimated_revenue' in data and 'completed_income_jobs' in data:
        data['avg_rev_per_job'] = round(data['estimated_revenue'] / data['completed_income_jobs'], 2) if data['completed_income_jobs'] != 0 else 0
    if 'total_booked' in data and 'leads_total' in data:
        data['booking_rate'] = round(data['total_booked'] / data['leads_total'], 2) if data['leads_total'] != 0 else 0
    if 'booked_converted' in data and 'opportunities_booked' in data:
        data['conversion_rate'] = round(data['booked_converted'] / data['opportunities_booked'], 2) if data['opportunities_booked'] != 0 else 0

    return data
