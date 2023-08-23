import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import sqldf
import itertools
from dotenv import load_dotenv
load_dotenv()
import openai
import os
import emoji
import time
import uuid

openai.api_key  = os.getenv('OPENAI_API_KEY')

contents = {
    'send_time':"""send_time mean the time we send an email campaign to user""",
    'is_emoji':"""is_emoji indicates that if the value 1 means there are emoji in the email, and if the value is 0 means that there are no emoji in the email.""",
    'subject_length_below_50':"""subject_length_below_50 indicates indicates that if the value is 1 means the subject of the email below 50, and if the value is 0 means the subject of the email above or equal 50.""",
    'Tags' : """Tags indicates that if the the value is EB means 'Ever Buy' and NB 'Never Buy'"""
}

metrics = {'open_rate':"""open_rate is a metrics that measures the percentage rate at which emails are opened"""
           }

def is_emojii(text):
    return emoji.emoji_count(text) > 0

def panjang(x) :
    if len(x) <= 50 :
        return 1
    return 0

def import_data():
    os.chdir('data')
    csv_files = [f for f in os.listdir() if f.endswith('.csv')]
    dfs = []
    for csv in csv_files:
        df = pd.read_csv(csv)
        dfs.append(df)
    final_df = pd.concat(dfs, ignore_index=True)
    final_df['is_emoji'] = final_df['Subject'].apply(is_emojii)
    final_df['subject_length_below_50'] = final_df['Subject'].apply(lambda x: panjang(x))
    final_df.rename(columns={"Send Date": "send_date", "Send Time": "send_time", "Open Rate": "open_rate",
                       "Click Rate": "click_rate"}, inplace=True)
    os.chdir('..')
    return final_df


def get_combinations(number_of_pairs = 2):
    combinations = list(itertools.combinations(contents, number_of_pairs))
    result = []
    for item1 in combinations:
        for item2 in metrics:
            result.append((list(item1), item2))
    return result

def get_completion(prompt, model="gpt-3.5-turbo-16k-0613"):
    messages = [{"role": "user", "content": prompt}]
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=0,
    )
    return response.choices[0].message["content"]

if __name__ == '__main__':
    myuuid = uuid.uuid4()
    # df_campaign = pd.read_csv('../JUL2023/2023_JULY_CAMPAIGN.csv')
    # df_campaign.rename(columns={"Day of Week": "days_of_week", "Open Rate": "open_rate"}, inplace=True)
    df_campaign = import_data()
    df_results = []
    for metric in metrics:
        result = df_campaign.corr().loc[metric][:].reset_index().to_string()
        df_results.append(f'Correlation Matrix of {metric}: {result}')
    for i in get_combinations(number_of_pairs=2):
        list_of_columns = i[0]
        columns = ', '.join(list_of_columns)
        value = i[1]
        df_temp = sqldf.run(f"""select {columns}, AVG({value}) as avg_{value} from df_campaign where {value} is not null group by {columns}""")
        df_max = sqldf.run(f"""select {columns}, avg_{value} as max_{value} from df_temp order by avg_{value} desc limit 1""")
        df_min = sqldf.run(f"""select {columns}, avg_{value} as min_{value} from df_temp order by avg_{value} asc limit 1""")
        # print(df_temp.to_string())
        df_results.append([f'Average of {value} from grouping {columns}: {df_temp.to_string()}',
                           f'Maximum Average of {value} from grouping {columns}: {df_max.to_string()}',
                           f'Minimum Average of {value} from grouping {columns}:{df_min.to_string()}'])
    result_responses = []
    total_fee = []
    total_execution_time = []
    f = open(f'result/{myuuid}_log.txt', 'w')
    for idx, data in enumerate(df_results):
        prompt = f"""I want you to act as Senior Data Analyst. \
        CRM email campaign performance data: ```{data}``` \
        Based on the provided datas about CRM email campaign performance data, delimited by triple, \
        I want you to give me as detail as possible about insights of {','.join(metrics)} based on the provided CRM email campaign performance datas. \
        Finally, give me your recommendations of what actions we need to take based on your previous insights to improve CRM email campaign performance, \
        and mention the impact percentage approx in number each of your recommendations actions. \
        For further details of the columns you can refer to columns' description delimited by triple backticks. \
        The columns' description: ```{' '.join(contents.values())} {' '.join(metrics.values())}``` \
        Your respond is one long answer or long explanation combined. \
        """

        start_time = time.time()
        response = get_completion(prompt)
        result_responses.append(f'**RESULT {idx + 1}** \n {response}')
        total_input_token = (len(prompt)/75) * 100
        total_output_token =(len(response)/75) * 100
        fee = ((total_input_token/1000) * 0.003) + ((total_output_token/1000) * 0.004)
        time_exec = time.time() - start_time
        total_fee.append(fee)
        total_execution_time.append(time_exec)
        print_log = f'Complete **RESULT {idx + 1}**. Input Token: {total_input_token } tokens, Output token: {total_output_token} tokens, Fee = ${fee}, Time Execution: {time_exec}s\n'
        print(print_log)
        f.write(print_log)
    print_log_final = f'Finished with Total Fee: ${sum(total_fee)} and Total Execution Time: {sum(total_execution_time)}s'
    print(print_log_final)
    f.write(print_log_final)
    f.close()

    fp = open(f'result/{myuuid}.txt', 'w')
    fp.write('\n'.join(result_responses))
    fp.close()