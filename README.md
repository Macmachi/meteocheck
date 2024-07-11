# 🌦 MeteoCheck 

### 📌 Description 
* 🌡️ Records weather details such as temperature, precipitation, 🌬️ wind speed, ☀️ UV index, 📊 atmospheric pressure and 💧 humidity using the [open-meteo.com API](https://open-meteo.com/en/docs).
* 🕰️ Operates 24/7, updating every hour.
* 🚨 Sends weather alerts to a Telegram bot by:
  * Checking every minute the weather for the upcoming 6️⃣ hours.
  * Monitoring every minute the atmospheric pressure for the next 2️⃣4️⃣ hours.
  * For each type of alert, an alert is sent only once a day
* 📊 Dispatches monthly and yearly weather summaries.
* [NEW] Command /weather to view the last entry in the csv (past hour)
* [NEW] Command /month to view report of the last month  
* [NEW] Command /year to view report of the current year  
* [NEW] Command /forecast to view the next 6 hours forecast
* [NEW] Command /sunshine to view sunshine reports

### 🔗 Useful Links
* 🔗 [Generate an API link for your city](https://open-meteo.com/en/docs).

## 🚀 Upcoming Features or Changes
* [✅] Improved support for the transition between summer and winter time.  
* [✅] Add the recently introduced 'humidity' parameter to monthly and annual reports, including the least humid and most humid days.
* [✅] Add a command to view weather forecasts for the upcoming 6 hours.
* [✅] Calculate the number of sunshine hours per day for the monthly report and per month for the annual report, based on UV values.
* [✅] Add emojis for reports.
* Improved presentation for information with /weather and forecast commands, such as monthly or yearly reports.
* Add command to view a report on all recorded data and also for sunshine?
* [Nice to have] Add a custom command to view a report between two dates, and if no data is available, display "No data available for the selected period"

## 🪱 Bug
* [FutureWarning] Setting an item of incompatible dtype is deprecated and will raise in a future error of pandas. Value '['2024-07-07T22:00:00Z']' has dtype incompatible with datetime64[ns, UTC], please explicitly cast to a compatible dtype first
  
