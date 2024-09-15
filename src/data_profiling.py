# import pandas as pd
# from ydata_profiling import ProfileReport
# import os

# # Load the dataset
# input_file = 'data/raw/iris.csv'
# output_file = 'iris_profile_report.html'

# # Check if the input file exists
# if os.path.exists(input_file):
#     df = pd.read_csv(input_file)
    
#     # Generate the profile report
#     profile = ProfileReport(df, title="Pandas Profiling Report", explorative=True)
    
#     # Save the report to an HTML file
#     profile.to_file(output_file)
#     print(f"Profile report saved to {output_file}")
# else:
#     print(f"File not found: {input_file}")

import pandas as pd
from ydata_profiling import ProfileReport
import os

def profile_data():
    """Generate and save a data profiling report."""
    input_file = 'data/processed/iris_cleaned.csv'
    output_file = 'iris_profile_report.html'

    # Check if the input file exists
    if os.path.exists(input_file):
        df = pd.read_csv(input_file)
        
        # Generate the profile report
        profile = ProfileReport(df, title="Pandas Profiling Report", explorative=True)
        
        # Save the report to an HTML file
        profile.to_file(output_file)
        print(f"Profile report saved to {output_file}")
    else:
        print(f"File not found: {input_file}")

if __name__ == "__main__":
    profile_data()
