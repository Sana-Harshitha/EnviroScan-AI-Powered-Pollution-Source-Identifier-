
import streamlit as st

st.title("🚀 EnviroScan Pollution Source Identifier")

city = st.text_input("Enter a city name", "Delhi")
if st.button("Analyze"):
    if not city.strip():
        st.warning("Please enter a valid city name.")
    else:
        st.success(f"Analyzing pollution sources for: {city}")
        st.markdown('''
        ### AI Analysis Results for **{city}** (Simulated)
        - **Main Pollutants:** PM2.5, NOx, SO2
        - **Likely Sources:**
          - Vehicle emissions
          - Industrial activity
          - Biomass/garbage burning
        - **Air Quality Index (AQI):** 185 (Unhealthy)
        - **Recommendation:** Limit outdoor activity. Use masks. Air purifiers recommended indoors.
        '''.format(city=city))
