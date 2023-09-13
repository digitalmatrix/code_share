import pandas as pd
import matplotlib.pyplot as plt

def read_raw_data_team_member():

    # read team member data
    df_team_member = pd.read_csv("team_member_data.csv")
    print(f"Team Member - Total Records: {len(df_team_member)}")

    return df_team_member

def read_raw_data_time_tracking():

    # read time tracking data
    df_time_tracking = pd.read_csv("time_tracking_data.csv")
    print(f"Time Tracking - Total Records: {len(df_time_tracking)}")

    # remove non billable tracking
    df_time_tracking = df_time_tracking[df_time_tracking["Billable"]=="Yes"]
    print(f"After removing non billable records, the records count is {len(df_time_tracking)}")

    # remove non week day tracking
    df_time_tracking["Date"] = pd.to_datetime(df_time_tracking["Date"])
    df_time_tracking["IsWeekDay"] = df_time_tracking["Date"].dt.weekday < 5
    df_time_tracking = df_time_tracking[df_time_tracking["IsWeekDay"] == True]
    print(f"After removing weekend records, the records count is {len(df_time_tracking)}")

    # finding the min & max date in the dataset
    print(f"Min Date is {df_time_tracking['Date'].min()}")
    print(f"Max Date is {df_time_tracking['Date'].max()}")

    return df_time_tracking

def create_staging_tables():

    # df_tm = read_raw_data_team_member()
    # df_tt = read_raw_data_time_tracking()
    
    # persist the raw data as a table here
    
    return None

def create_dim_table():

    # read raw data
    df_tm = read_raw_data_team_member()
    df_tt = read_raw_data_time_tracking()

    # set index to the dataframes
    index_cols = ["Name", "Role"]
    df_tm = df_tm.set_index(index_cols)
    df_tt = df_tt.set_index(index_cols)

    # join dataframes - Time Tracking Left Joins with Team Member
    df_btm_tt = df_tt.join(df_tm, how="left")

    # add two columns to the new dataset - Weekly & Monthly Date
    df_btm_tt["Weekly"] = df_btm_tt['Date'].dt.to_period('W').dt.start_time
    df_btm_tt["Monthly"] = df_btm_tt['Date'].dt.to_period('M').dt.start_time

    # reset Index to re-gain the index columns
    df_btm_tt = df_btm_tt.reset_index()

    return df_btm_tt

def create_weekly_fct_table(df_dim):

    # select the required columns from the Dim table
    df_weekly = df_dim[["Name", "Role", "Weekly", "Freelancer", "Billable", "Hours", "Weekly Capacity", "Weekly Billable Capacity"]]

    # group by the target columns and aggregate the value columns
    df_weekly_grouped = df_weekly.groupby(["Weekly", "Name", "Role"]).agg(
                                                                            {
                                                                                "Weekly Capacity": max,
                                                                                "Weekly Billable Capacity": max,
                                                                                "Hours": sum
                                                                            }
                                                                        ).reset_index()
    # rename the aggregate columns
    df_weekly_grouped.columns = ["Weekly", "Name", "Role", "Capacity", "Billable Capacity", "Billable Hours"]


    # again group by the target columns that matches the fct table requirements and aggregate the rest
    df_fct_weekly_grouped = df_weekly_grouped.groupby(["Weekly", "Role"]).agg(
                                                                                {
                                                                                    "Capacity": sum,
                                                                                    "Billable Capacity": sum,
                                                                                    "Billable Hours": sum
                                                                                }
                                                                            ).reset_index()
    
    # rename the aggregate columns
    df_fct_weekly_grouped.columns = ["Week", "Role", "Capacity", "Billable Capacity", "Billable Hours"]

    # sort the dataframe by Week date and the role
    df_fct_weekly_sorted = df_fct_weekly_grouped.sort_values(by=["Week", "Role"])

    # persist the fct table - fct_weekly
    print(f"fct weekly - Records: {len(df_fct_weekly_sorted)}")
    df_fct_weekly_sorted.to_csv("fct_weekly.csv")

    # return the fct table
    return df_fct_weekly_sorted

def create_monthly_fct_table(df_dim):

    # select the required columns from the Dim table
    df_monthly = df_dim[["Name", "Role", "Monthly", "Freelancer", "Billable", "Hours", "Weekly Capacity", "Weekly Billable Capacity"]]

    # group by the target columns and aggregate the value columns
    df_monthly_grouped = df_monthly.groupby(["Monthly", "Name", "Role"]).agg(
                                                                                {
                                                                                    "Weekly Capacity": max,
                                                                                    "Weekly Billable Capacity": max,
                                                                                    "Hours": sum
                                                                                }
                                                                            ).reset_index()
    
    # rename the aggregate columns
    df_monthly_grouped.columns = ["Monthly", "Name", "Role", "Capacity", "Billable Capacity", "Billable Hours"]

    # again group by the target columns that matches the fct table requirements and aggregate the rest
    df_fct_monthly_grouped = df_monthly_grouped.groupby(["Monthly", "Role"]).agg(
                                                                                    {
                                                                                        "Capacity": sum,
                                                                                        "Billable Capacity": sum,
                                                                                        "Billable Hours": sum
                                                                                    }
                                                                                ).reset_index()
    
    # rename the aggregate columns
    df_fct_monthly_grouped.columns = ["Month", "Role", "Capacity", "Billable Capacity", "Billable Hours"]

    # sort the dataframe by Month date and the role
    df_fct_monthly_sorted = df_fct_monthly_grouped.sort_values(by=["Month", "Role"])

    # persist the fct table - fct_monthly
    print(f"fct monthly - Records: {len(df_fct_monthly_sorted)}")
    df_fct_monthly_sorted.to_csv("fct_monthly.csv")

    # return the fct table
    return df_fct_monthly_sorted

def create_fct_tables():

    fct_tables = []

    # create and retrieve dim table
    df_dim = create_dim_table()
    
    # create and add the fct tables to the list
    fct_tables.append(create_weekly_fct_table(df_dim))
    fct_tables.append(create_monthly_fct_table(df_dim))

    # return the list
    return fct_tables

def plot_graph(target_df, df_type):

    # get all the roles from the weekly table
    roles = target_df["Role"].unique()

    # plot graph for each role and for the last 3 months data
    for role in roles:

        output_file_name = df_type + "_" + role
        output_file = output_file_name + ".jpg"

        # filter role
        df_for_graph = target_df[target_df["Role"] == role]

        # filter last 3 months data
        if df_type == "Weekly":
            df_for_graph = df_for_graph[df_for_graph["Week"] >= '2021-03-01']
            df_for_graph = df_for_graph[["Week", "Capacity", "Billable Capacity", "Billable Hours"]]
            df_for_graph["date_col"] = pd.to_datetime(df_for_graph["Week"]).dt.date

        else:
            df_for_graph = df_for_graph[df_for_graph["Month"] >= '2021-03-01']
            df_for_graph = df_for_graph[["Month", "Capacity", "Billable Capacity", "Billable Hours"]]
            df_for_graph["date_col"] = pd.to_datetime(df_for_graph["Month"]).dt.date

        # plot graph
        df_for_graph.plot(
                            x="date_col", 
                            y=["Capacity", "Billable Capacity", "Billable Hours"],
                            kind="barh", 
                            stacked=True,
                            title=output_file_name.replace("_", " - "),
                            xlabel="",
                            ylabel=""
                        )
        
        # save graph
        plt.savefig(output_file, bbox_inches='tight')

def create_report():
    fct_tables = create_fct_tables()

    # draw weekly reports
    fct_weekly = fct_tables[0]
    plot_graph(fct_weekly, "Weekly")

    fct_monthly = fct_tables[1]
    plot_graph(fct_monthly, "Monthly")
    
    #draw_graph(fct_tables[1][["Month", "Capacity", "Billable Capacity", "Billable Hours"]], "bar")
    return None

def main():
    create_report()

if __name__ == "__main__":
    main()