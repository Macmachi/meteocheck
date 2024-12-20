# 🌦 MeteoCheck 

### 📌 Description 
* 🌡️ Records weather details such as temperature, precipitation, 🌬️ wind speed, ☀️ UV index, 📊 atmospheric pressure and 💧 humidity using the [open-meteo.com API](https://open-meteo.com/en/docs).
* 🕰️ Operates 24/7, updating every hour.
* 🚨 Sends weather alerts to a Telegram bot by:
  * Checking every minute the weather for the upcoming 6️⃣ hours.
  * Monitoring every minute the atmospheric pressure for the next 2️⃣4️⃣ hours.
  * For each type of alert, an alert is sent only once a day
* 📊 Dispatches monthly and yearly weather summaries.
* Command /start to start receiving alerts (& List of commands)
* Command /weather to view the last entry in the CSV (past hour)
* Command /month to view report of the last month's entries in the CSV
* Command /year to view report of the current year's entries in the CSV
* [NEW] Command /all to view report of all entries in the CSV
* Command /forecast to view the next 6 hours forecast
* Command /sunshine to view sunshine reports of all entries in the CSV 
  
### 🔗 Useful Links
* 🔗 [Generate an API link for your city](https://open-meteo.com/en/docs).

## 🚀 Upcoming Features or Changes
* [✅] Improved support for the transition between summer and winter time.  
* [✅] Add the recently introduced 'humidity' parameter to monthly and annual reports, including the least humid and most humid days.
* [✅] Add a command to view weather forecasts for the upcoming 6 hours.
* [✅] Calculate the number of sunshine hours per day for the monthly report and per month for the annual report, based on UV values.
* [✅] Add emojis for reports.
* [✅] Improved presentation for information with /weather and forecast commands, such as monthly or yearly reports.
* [✅] Add command to view a report on all recorded data
* [Nice to have] Add a custom command to view a report between two dates, and if no data is available, display "No data available for the selected period"

### ⚠️ Important Note on Weather Data Timing
Attention: The script records weather data based on the server time. For example, if the server is set to UTC+0, then imagine it’s 12:00 (UTC+0). If we request the weather from the last hour at 13:00 (UTC+1), the bot will tell us that at 12:00 UTC+0 it was this, but in fact, it returns the entry for the last hour that has just finished, which is from 11:00 to 12:00.

## 🪱 Bug
* Check logs 
  
