import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv # If you're using .env for credentials

# --- Configuration ---
# Load environment variables if you're using a .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env')) # Assumes .env is in the project root

DB_HOST = os.getenv("PostgreSQL_HOST")
DB_PORT = os.getenv("PostgreSQL_PORT", "5432")
DB_NAME = os.getenv("PostgreSQL_DBNAME")
DB_USER = os.getenv("PostgreSQL_USER")
DB_PASSWORD = os.getenv("PostgreSQL_PWD")
DB_SCHEMA = os.getenv("POSTGRESQL_SCHEMA", "CRDCdata") # Your target schema

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'plots')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print(f"Successfully connected to PostgreSQL database: {DB_NAME}")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None

def fetch_data_from_db(conn, query):
    """Fetches data from the database using the given query."""
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()


def main():
    conn = get_db_connection()
    if not conn:
        raise Exception("Database connection failed.")

    # Table name from sanitized file
    algebra_table_name = "tbl_2017_18_crdc_data_algebra_i"

    # Properly formatted and cast-safe SQL query
    query_algebra_pass_rates = sql.SQL("""
        SELECT
            lea_state, -- Changed grouping to lea_state
            SUM(CASE WHEN tot_algenr_gs1112_m::INTEGER >= 0 THEN tot_algenr_gs1112_m::INTEGER ELSE 0 END) AS total_algebra_enroll_male_gs11_12, -- Added male enrollment
            SUM(CASE WHEN tot_algenr_gs1112_f::INTEGER >= 0 THEN tot_algenr_gs1112_f::INTEGER ELSE 0 END) AS total_algebra_enroll_female_gs11_12, -- Added female enrollment
            SUM(CASE WHEN tot_algpass_gs1112_m::INTEGER >= 0 THEN tot_algpass_gs1112_m::INTEGER ELSE 0 END) AS total_algebra_pass_male_gs11_12,
            SUM(CASE WHEN tot_algpass_gs1112_f::INTEGER >= 0 THEN tot_algpass_gs1112_f::INTEGER ELSE 0 END) AS total_algebra_pass_female_gs11_12,
            SUM(
                CASE WHEN tot_algenr_gs1112_m::INTEGER >= 0 THEN tot_algenr_gs1112_m::INTEGER ELSE 0 END +
                CASE WHEN tot_algenr_gs1112_f::INTEGER >= 0 THEN tot_algenr_gs1112_f::INTEGER ELSE 0 END
            ) AS total_algebra_enroll_combined_gs11_12, -- Added combined enrollment
            SUM(
                CASE WHEN tot_algpass_gs1112_m::INTEGER >= 0 THEN tot_algpass_gs1112_m::INTEGER ELSE 0 END +
                CASE WHEN tot_algpass_gs1112_f::INTEGER >= 0 THEN tot_algpass_gs1112_f::INTEGER ELSE 0 END
            ) AS total_algebra_pass_combined_gs11_12
        FROM
            {schema}.{table_name}
        WHERE -- Assuming lea_state_name is a column in this table
            tot_algenr_gs1112_m::INTEGER >= -1 AND tot_algenr_gs1112_f::INTEGER >= -1 AND -- Filter out negative enrollment codes
            tot_algpass_gs1112_m::INTEGER >= -1 AND tot_algpass_gs1112_f::INTEGER >= -1 -- Filter out negative pass codes
        GROUP BY
            lea_state -- Changed grouping
        ORDER BY
            lea_state; -- Changed ordering
    """).format(
        schema=sql.Identifier(DB_SCHEMA),
        table_name=sql.Identifier(algebra_table_name)
    )

    # Execute query and fetch results
    print(f"\nExecuting query for Algebra I pass rates from table '{DB_SCHEMA}.{algebra_table_name}'...")
    df_algebra_data = fetch_data_from_db(conn, query_algebra_pass_rates.as_string(conn))

    # Display results
    if not df_algebra_data.empty:
        print(f"\n--- Algebra I Enrollment and Pass Data by State (Top 5 rows) ---")
        print(df_algebra_data.head()) # This will now show data aggregated by state


        # --- Scatter plot of Combined Gender Total Enrollment vs. Passing (All States) ---
        plt.figure(figsize=(12, 8))
        sns.regplot(data=df_algebra_data, 
                    x='total_algebra_enroll_combined_gs11_12', 
                    y='total_algebra_pass_combined_gs11_12', 
                    scatter_kws={'alpha':0.6, 's': 50}, 
                    line_kws={'color':'green'})
        # Optional: Add state annotations for all states (can be crowded)
        # for i in range(df_algebra_data.shape[0]):
        #     plt.text(x=df_algebra_data['total_algebra_enroll_combined_gs11_12'].iloc[i], 
        #              y=df_algebra_data['total_algebra_pass_combined_gs11_12'].iloc[i], 
        #              s=df_algebra_data['lea_state'].iloc[i], 
        #              fontdict=dict(color='black',size=8), ha='left', va='bottom')
        plt.title('All States: Combined Algebra I Enrollment vs. Passing (Grades 11-12)')
        plt.xlabel('Total Combined Algebra I Enrollment')
        plt.ylabel('Total Combined Algebra I Passing')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_combined_enroll_vs_pass_scatter_all_states.png'))
        plt.close()
        print(f"Scatter plot for combined enrollment vs. passing (All States) saved to {os.path.join(OUTPUT_DIR, 'algebra_combined_enroll_vs_pass_scatter_all_states.png')}")

        # --- Prepare data for Top 15 and Bottom 15 states by combined enrollment ---
        df_sorted_by_combined_enrollment = df_algebra_data.sort_values('total_algebra_enroll_combined_gs11_12', ascending=False)

        if len(df_sorted_by_combined_enrollment) >= 15: # Ensure there are at least 15 states
            # --- Plot Scatter of Combined Gender Total Enrollment vs. Passing (Top 15 States) ---
            df_top_15_combined_enrollment = df_sorted_by_combined_enrollment.head(15)
            plt.figure(figsize=(12, 8))
            sns.regplot(data=df_top_15_combined_enrollment, 
                        x='total_algebra_enroll_combined_gs11_12', 
                        y='total_algebra_pass_combined_gs11_12', 
                        scatter_kws={'alpha':0.6, 's': 50}, # Slightly larger points 
                        line_kws={'color':'red'})
            for i in range(df_top_15_combined_enrollment.shape[0]):
                plt.text(x=df_top_15_combined_enrollment['total_algebra_enroll_combined_gs11_12'].iloc[i], 
                         y=df_top_15_combined_enrollment['total_algebra_pass_combined_gs11_12'].iloc[i], 
                         s=df_top_15_combined_enrollment['lea_state'].iloc[i], 
                         fontdict=dict(color='black',size=9), ha='left', va='bottom') # Adjust alignment
            plt.title('Top 15 States by Enrollment: Combined Algebra I Enrollment vs. Passing')
            plt.xlabel('Total Combined Algebra I Enrollment')
            plt.ylabel('Total Combined Algebra I Passing')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_combined_enroll_vs_pass_scatter_top15_states.png'))
            plt.close()
            print(f"Scatter plot for combined enrollment vs. passing (Top 15 States) saved to {os.path.join(OUTPUT_DIR, 'algebra_combined_enroll_vs_pass_scatter_top15_states.png')}")

            # --- Plot Scatter of Combined Gender Total Enrollment vs. Passing (Bottom 15 States) ---
            df_bottom_15_combined_enrollment = df_sorted_by_combined_enrollment.tail(15)
            plt.figure(figsize=(12, 8))
            sns.regplot(data=df_bottom_15_combined_enrollment, 
                        x='total_algebra_enroll_combined_gs11_12', 
                        y='total_algebra_pass_combined_gs11_12', 
                        scatter_kws={'alpha':0.6, 's': 50}, 
                        line_kws={'color':'blue'}) # Different line color for bottom
            for i in range(df_bottom_15_combined_enrollment.shape[0]):
                plt.text(x=df_bottom_15_combined_enrollment['total_algebra_enroll_combined_gs11_12'].iloc[i], 
                         y=df_bottom_15_combined_enrollment['total_algebra_pass_combined_gs11_12'].iloc[i], 
                         s=df_bottom_15_combined_enrollment['lea_state'].iloc[i], 
                         fontdict=dict(color='black',size=9), ha='left', va='bottom')
            plt.title('Bottom 15 States by Enrollment: Combined Algebra I Enrollment vs. Passing')
            plt.xlabel('Total Combined Algebra I Enrollment')
            plt.ylabel('Total Combined Algebra I Passing')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_combined_enroll_vs_pass_scatter_bottom15_states.png'))
            plt.close()
            print(f"Scatter plot for combined enrollment vs. passing (Bottom 15 States) saved to {os.path.join(OUTPUT_DIR, 'algebra_combined_enroll_vs_pass_scatter_bottom15_states.png')}")
        else:
            print("\nSkipping Top/Bottom 15 Combined Enrollment vs. Passing scatter plots: Not enough states (need at least 15).")

        # --- Scatter plot of Male Enrollment vs. Passing (All States) ---
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df_algebra_data, 
                        x='total_algebra_enroll_male_gs11_12', 
                        y='total_algebra_pass_male_gs11_12', 
                        alpha=0.6)
        plt.title('Male Algebra I Enrollment vs. Passing (Grades 11-12) by State (All States)')
        plt.xlabel('Total Male Algebra I Enrollment')
        plt.ylabel('Total Male Algebra I Passing')
        plt.grid(True)
        plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_male_enroll_vs_pass_scatter_all_states.png'))
        plt.close()
        print(f"Male enrollment vs. passing scatter plot (All States) saved to {os.path.join(OUTPUT_DIR, 'algebra_male_enroll_vs_pass_scatter_all_states.png')}")

        # --- Plot Scatter of Female Enrollment vs. Passing (All States) ---
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df_algebra_data, 
                        x='total_algebra_enroll_female_gs11_12', 
                        y='total_algebra_pass_female_gs11_12', 
                        alpha=0.6)
        plt.title('Female Algebra I Enrollment vs. Passing (Grades 11-12) by State (All States)')
        plt.xlabel('Total Female Algebra I Enrollment')
        plt.ylabel('Total Female Algebra I Passing')
        plt.grid(True)
        plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_female_enroll_vs_pass_scatter_all_states.png'))
        plt.close()
        print(f"Female enrollment vs. passing scatter plot (All States) saved to {os.path.join(OUTPUT_DIR, 'algebra_female_enroll_vs_pass_scatter_all_states.png')}")


        # --- Prepare Data for Top 15 and Bottom 15 Plots (based on combined enrollment) ---
        df_sorted_by_combined_enrollment = df_algebra_data.sort_values('total_algebra_enroll_combined_gs11_12', ascending=False)

        if len(df_sorted_by_combined_enrollment) >= 15: # Ensure enough states for distinct top/bottom 15
            df_top_15_enrollment = df_sorted_by_combined_enrollment.head(15) # Changed from 10 to 15
            df_bottom_15_enrollment = df_sorted_by_combined_enrollment.tail(15) # Changed from 10 to 15

            # --- Plot Scatter of Male Enrollment vs. Passing (Top 15 States by Combined Enrollment) ---
            plt.figure(figsize=(10, 6))
            sns.scatterplot(data=df_top_15_enrollment, # Changed DataFrame
                            x='total_algebra_enroll_male_gs11_12', 
                            y='total_algebra_pass_male_gs11_12', 
                            alpha=0.8, s=60) # Slightly larger points for fewer data points
            # Optional: Add state labels for top 15
            for i in range(df_top_15_enrollment.shape[0]): # Changed DataFrame
                 plt.text(x=df_top_15_enrollment['total_algebra_enroll_male_gs11_12'].iloc[i], # Changed DataFrame
                          y=df_top_15_enrollment['total_algebra_pass_male_gs11_12'].iloc[i], # Changed DataFrame
                          s=df_top_15_enrollment['lea_state'].iloc[i], # Changed DataFrame
                          fontdict=dict(color='black',size=9), ha='left', va='bottom')
            plt.title('Top 15 States by Enrollment: Male Algebra I Enrollment vs. Passing') # Changed title
            plt.xlabel('Total Male Algebra I Enrollment')
            plt.ylabel('Total Male Algebra I Passing')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_male_enroll_vs_pass_scatter_top15_states.png')) # Changed filename
            plt.close()
            print(f"Male enrollment vs. passing scatter plot (Top 15 States) saved to {os.path.join(OUTPUT_DIR, 'algebra_male_enroll_vs_pass_scatter_top15_states.png')}") # Changed print message

            # --- Plot Scatter of Female Enrollment vs. Passing (Top 15 States by Combined Enrollment) ---
            plt.figure(figsize=(10, 6))
            sns.scatterplot(data=df_top_15_enrollment, # Changed DataFrame
                            x='total_algebra_enroll_female_gs11_12', 
                            y='total_algebra_pass_female_gs11_12', 
                            alpha=0.8, s=60)
            # Optional: Add state labels for top 15
            for i in range(df_top_15_enrollment.shape[0]): # Changed DataFrame
                 plt.text(x=df_top_15_enrollment['total_algebra_enroll_female_gs11_12'].iloc[i], # Changed DataFrame
                          y=df_top_15_enrollment['total_algebra_pass_female_gs11_12'].iloc[i], # Changed DataFrame
                          s=df_top_15_enrollment['lea_state'].iloc[i], # Changed DataFrame
                          fontdict=dict(color='black',size=9), ha='left', va='bottom')
            plt.title('Top 15 States by Enrollment: Female Algebra I Enrollment vs. Passing') # Changed title
            plt.xlabel('Total Female Algebra I Enrollment')
            plt.ylabel('Total Female Algebra I Passing')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_female_enroll_vs_pass_scatter_top15_states.png')) # Changed filename
            plt.close()
            print(f"Female enrollment vs. passing scatter plot (Top 15 States) saved to {os.path.join(OUTPUT_DIR, 'algebra_female_enroll_vs_pass_scatter_top15_states.png')}") # Changed print message

            # --- Plot Scatter of Male Enrollment vs. Passing (Bottom 15 States by Combined Enrollment) ---
            plt.figure(figsize=(10, 6))
            sns.scatterplot(data=df_bottom_15_enrollment, # Changed DataFrame
                            x='total_algebra_enroll_male_gs11_12', 
                            y='total_algebra_pass_male_gs11_12', 
                            alpha=0.8, s=60)
            # Optional: Add state labels for bottom 15
            for i in range(df_bottom_15_enrollment.shape[0]): # Changed DataFrame
                 plt.text(x=df_bottom_15_enrollment['total_algebra_enroll_male_gs11_12'].iloc[i], # Changed DataFrame
                          y=df_bottom_15_enrollment['total_algebra_pass_male_gs11_12'].iloc[i], # Changed DataFrame
                          s=df_bottom_15_enrollment['lea_state'].iloc[i], # Changed DataFrame
                          fontdict=dict(color='black',size=9), ha='left', va='bottom')
            plt.title('Bottom 15 States by Enrollment: Male Algebra I Enrollment vs. Passing') # Changed title
            plt.xlabel('Total Male Algebra I Enrollment')
            plt.ylabel('Total Male Algebra I Passing')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_male_enroll_vs_pass_scatter_bottom15_states.png')) # Changed filename
            plt.close()
            print(f"Male enrollment vs. passing scatter plot (Bottom 15 States) saved to {os.path.join(OUTPUT_DIR, 'algebra_male_enroll_vs_pass_scatter_bottom15_states.png')}") # Changed print message

            # --- Plot Scatter of Female Enrollment vs. Passing (Bottom 15 States by Combined Enrollment) ---
            plt.figure(figsize=(10, 6))
            sns.scatterplot(data=df_bottom_15_enrollment, # Changed DataFrame
                            x='total_algebra_enroll_female_gs11_12', 
                            y='total_algebra_pass_female_gs11_12', 
                            alpha=0.8, s=60)
            # Optional: Add state labels for bottom 15
            for i in range(df_bottom_15_enrollment.shape[0]): # Changed DataFrame
                 plt.text(x=df_bottom_15_enrollment['total_algebra_enroll_female_gs11_12'].iloc[i], # Changed DataFrame
                          y=df_bottom_15_enrollment['total_algebra_pass_female_gs11_12'].iloc[i], # Changed DataFrame
                          s=df_bottom_15_enrollment['lea_state'].iloc[i], # Changed DataFrame
                          fontdict=dict(color='black',size=9), ha='left', va='bottom')
            plt.title('Bottom 15 States by Enrollment: Female Algebra I Enrollment vs. Passing') # Changed title
            plt.xlabel('Total Female Algebra I Enrollment')
            plt.ylabel('Total Female Algebra I Passing')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_female_enroll_vs_pass_scatter_bottom15_states.png')) # Changed filename
            plt.close()
            print(f"Female enrollment vs. passing scatter plot (Bottom 15 States) saved to {os.path.join(OUTPUT_DIR, 'algebra_female_enroll_vs_pass_scatter_bottom15_states.png')}") # Changed print message

        else:
            print("\nSkipping Top/Bottom 15 Enrollment vs. Passing scatter plots: Not enough states (need at least 15).") # Updated print message



        # --- Plot Bar Chart of Pass Rates by State ---
        # Calculate pass rate, handle potential division by zero
        df_algebra_data['pass_rate_combined_gs11_12'] = df_algebra_data.apply(
            lambda row: (row['total_algebra_pass_combined_gs11_12'] / row['total_algebra_enroll_combined_gs11_12']) * 100
            if row['total_algebra_enroll_combined_gs11_12'] > 0 else 0,
            axis=1
        )
        # Sort by pass rate for better visualization
        df_sorted_by_pass_rate_all = df_algebra_data.sort_values('pass_rate_combined_gs11_12', ascending=False)

        # Plot Top 15 States by Pass Rate
        if len(df_sorted_by_pass_rate_all) >= 15:
            df_top_15_pass_rate = df_sorted_by_pass_rate_all.head(15)
            plt.figure(figsize=(12, 7)) 
            sns.barplot(data=df_top_15_pass_rate, 
                        x='lea_state', 
                        y='pass_rate_combined_gs11_12', 
                        hue='lea_state',
                        palette='viridis', 
                        legend=False)
            plt.title('Top 15 States: Algebra I Combined Pass Rate (Grades 11-12)')
            plt.xlabel('State')
            plt.ylabel('Combined Pass Rate (%)')
            plt.xticks(rotation=90) 
            plt.tight_layout() 
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_pass_rate_top_15_states_barchart.png'))
            plt.close()
            print(f"Algebra pass rate (Top 15 States) bar chart saved to {os.path.join(OUTPUT_DIR, 'algebra_pass_rate_top_15_states_barchart.png')}")

        # Plot Bottom 15 States by Pass Rate
        if len(df_sorted_by_pass_rate_all) >= 15: # Check if there are at least 15 to get a distinct bottom 15
                                                 # If less than 30 total, top 15 and bottom 15 might overlap.
                                                 # For simplicity, we'll just take the tail.
            df_bottom_15_pass_rate = df_sorted_by_pass_rate_all.tail(15).sort_values('pass_rate_combined_gs11_12', ascending=True) # Sort ascending for bottom
            plt.figure(figsize=(12, 7)) 
            sns.barplot(data=df_bottom_15_pass_rate, 
                        x='lea_state', 
                        y='pass_rate_combined_gs11_12', 
                        hue='lea_state',
                        palette='viridis_r', # Reversed palette for bottom
                        legend=False)
            plt.title('Bottom 15 States: Algebra I Combined Pass Rate (Grades 11-12)')
            plt.xlabel('State')
            plt.ylabel('Combined Pass Rate (%)')
            plt.xticks(rotation=90) 
            plt.tight_layout() 
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_pass_rate_bottom_15_states_barchart.png'))
            plt.close()
            print(f"Algebra pass rate (Bottom 15 States) bar chart saved to {os.path.join(OUTPUT_DIR, 'algebra_pass_rate_bottom_15_states_barchart.png')}")

        # --- Plot Stacked Bar Chart of Enrollment vs. Passing by State ---
        # Calculate 'did not pass' for stacking
        df_algebra_data['did_not_pass_combined_gs11_12'] = df_algebra_data['total_algebra_enroll_combined_gs11_12'] - df_algebra_data['total_algebra_pass_combined_gs11_12']

        # Select and reorder columns for stacking, using lea_state as index for plotting
        df_stacked_chart = df_algebra_data.set_index('lea_state')[['total_algebra_pass_combined_gs11_12', 'did_not_pass_combined_gs11_12']]
        df_stacked_chart_renamed = df_stacked_chart.rename(columns={
            'total_algebra_pass_combined_gs11_12': 'Passed Algebra I',
            'did_not_pass_combined_gs11_12': 'Did Not Pass Algebra I (Enrolled)'
        })
        # Sort by total enrollment for this chart
        df_stacked_chart_renamed_sorted = df_algebra_data.sort_values('total_algebra_enroll_combined_gs11_12', ascending=False)
        
        # Plot Top 15 States by Enrollment for Stacked Chart
        if len(df_stacked_chart_renamed_sorted) >= 15:
            df_top_15_stacked = df_stacked_chart_renamed_sorted.head(15).set_index('lea_state')[['total_algebra_pass_combined_gs11_12', 'did_not_pass_combined_gs11_12']].rename(columns={
                'total_algebra_pass_combined_gs11_12': 'Passed Algebra I',
                'did_not_pass_combined_gs11_12': 'Did Not Pass Algebra I (Enrolled)'
            })
            df_top_15_stacked.plot(kind='bar', stacked=True, figsize=(12, 7), colormap='viridis')
            plt.title('Top 15 States by Enrollment: Algebra I Passed vs. Did Not Pass')
            plt.xlabel('State')
            plt.ylabel('Number of Students')
            plt.xticks(rotation=90)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_enroll_pass_stacked_top_15_barchart.png'))
            plt.close()
            print(f"Algebra enrollment vs. passing (Top 15 States by Enrollment) stacked bar chart saved to {os.path.join(OUTPUT_DIR, 'algebra_enroll_pass_stacked_top_15_barchart.png')}")

        # Plot Bottom 15 States by Enrollment for Stacked Chart
        if len(df_stacked_chart_renamed_sorted) >= 15:
            df_bottom_15_stacked = df_stacked_chart_renamed_sorted.tail(15).set_index('lea_state')[['total_algebra_pass_combined_gs11_12', 'did_not_pass_combined_gs11_12']].rename(columns={
                'total_algebra_pass_combined_gs11_12': 'Passed Algebra I',
                'did_not_pass_combined_gs11_12': 'Did Not Pass Algebra I (Enrolled)'
            })
            df_bottom_15_stacked.plot(kind='bar', stacked=True, figsize=(12, 7), colormap='viridis_r')
            plt.title('Bottom 15 States by Enrollment: Algebra I Passed vs. Did Not Pass')
            plt.xlabel('State')
            plt.ylabel('Number of Students')
            plt.xticks(rotation=90)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_enroll_pass_stacked_bottom_15_barchart.png'))
            plt.close()
            print(f"Algebra enrollment vs. passing (Bottom 15 States by Enrollment) stacked bar chart saved to {os.path.join(OUTPUT_DIR, 'algebra_enroll_pass_stacked_bottom_15_barchart.png')}")

        # --- Plot Grouped Bar Charts for Male vs. Female Pass Counts by State ---
        print("\n--- Generating Grouped Bar Charts for Male vs. Female Pass Counts ---")

        # Ensure numeric values
        df_algebra_data['total_algebra_pass_male_gs11_12'] = pd.to_numeric(df_algebra_data['total_algebra_pass_male_gs11_12'], errors='coerce')
        df_algebra_data['total_algebra_pass_female_gs11_12'] = pd.to_numeric(df_algebra_data['total_algebra_pass_female_gs11_12'], errors='coerce')

        # Aggregate by state in case multiple districts exist
        df_state_grouped = df_algebra_data.groupby('lea_state', as_index=False).agg({
            'total_algebra_pass_male_gs11_12': 'sum',
            'total_algebra_pass_female_gs11_12': 'sum'
        })
        df_state_grouped['total_algebra_pass_combined_gs11_12'] = (
            df_state_grouped['total_algebra_pass_male_gs11_12'] +
            df_state_grouped['total_algebra_pass_female_gs11_12']
        )

        # Melt for plotting
        df_melted_gender_pass = df_state_grouped.melt(
            id_vars='lea_state',
            value_vars=['total_algebra_pass_male_gs11_12', 'total_algebra_pass_female_gs11_12'],
            var_name='gender_metric',
            value_name='pass_count'
        )

        df_melted_gender_pass['gender'] = df_melted_gender_pass['gender_metric'].apply(
            lambda x: 'Male' if 'male' in x else 'Female'
        )

        # Make sure OUTPUT_DIR exists
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)

        # --- All States Plot ---
        plt.figure(figsize=(18, 10))
        sns.barplot(data=df_melted_gender_pass, x='lea_state', y='pass_count', hue='gender',
                    palette={'Male': 'skyblue', 'Female': 'lightcoral'})
        plt.title('Algebra I Pass Counts (Grades 11-12) by Gender and State (All States)')
        plt.xlabel('State')
        plt.ylabel('Number of Students Passing Algebra I')
        plt.xticks(rotation=90)
        plt.legend(title='Gender')
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_gender_pass_counts_grouped_all_states_barchart.png'))
        plt.close()

        print("Saved: Grouped bar chart for all states.")

        # --- Top/Bottom 15 by Combined ---
        df_sorted = df_state_grouped.sort_values('total_algebra_pass_combined_gs11_12', ascending=False)

        if len(df_sorted) >= 15:
            top_15_states = df_sorted.head(15)['lea_state'].tolist()
            bottom_15_states = df_sorted.tail(15)['lea_state'].tolist()

            # Filter top 15
            df_top_15 = df_melted_gender_pass[df_melted_gender_pass['lea_state'].isin(top_15_states)]
            plt.figure(figsize=(12, 7))
            sns.barplot(data=df_top_15, x='lea_state', y='pass_count', hue='gender',
                        palette={'Male': 'skyblue', 'Female': 'lightcoral'})
            plt.title('Top 15 States: Algebra I Pass Counts by Gender')
            plt.xlabel('State')
            plt.ylabel('Number of Students Passing Algebra I')
            plt.xticks(rotation=90)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_gender_pass_counts_grouped_top_15_barchart.png'))
            plt.close()

            # Filter bottom 15
            df_bottom_15 = df_melted_gender_pass[df_melted_gender_pass['lea_state'].isin(bottom_15_states)]
            plt.figure(figsize=(12, 7))
            sns.barplot(data=df_bottom_15, x='lea_state', y='pass_count', hue='gender',
                        palette={'Male': 'skyblue', 'Female': 'lightcoral'})
            plt.title('Bottom 15 States: Algebra I Pass Counts by Gender')
            plt.xlabel('State')
            plt.ylabel('Number of Students Passing Algebra I')
            plt.xticks(rotation=90)
            plt.tight_layout()
            plt.savefig(os.path.join(OUTPUT_DIR, 'algebra_gender_pass_counts_grouped_bottom_15_barchart.png'))
            plt.close()

            print("Saved: Top/Bottom 15 grouped bar charts.")

        else:
             print("\nSkipping Top/Bottom 15 Grouped Gender Pass Count plots: Not enough states (need at least 15).")
    else:
        print("No data returned.")

    # Close connection
    conn.close()
    print("Database connection closed.")

if __name__ == '__main__':
    main()