# 🌦 MeteoCheck 

### 📌 Description 
* 🌡️ Records weather details such as temperature, precipitation, 🌬️ wind speed, ☀️ UV index, 📊 atmospheric pressure and 💧 humidity using the [open-meteo.com API](https://open-meteo.com/en/docs).
* 🕰️ Operates 24/7, updating every hour.
* 🚨 Sends weather alerts to a Telegram bot by :
  * Checking the weather for the upcoming 6️⃣ hours every hour.
  * Monitoring the atmospheric pressure for the next 2️⃣4️⃣ hours.
  * For each type of alert, an alert is sent only once a day
* 📊 Dispatches monthly and yearly weather summaries.
* [NEW] Command /weather to view the last entry in the csv (past hour)

### 🔗 Useful Links
* 🔗 [Generate an API link for your city](https://open-meteo.com/en/docs).

## 🪱 Bug
* Don't send the monthly report on Telegram due to this bug: Error in end_of_month_summary: Can only use .dt accessor with datetimelike values. (monitored)

## 🚀 Upcoming Features or Changes
* Better support for the transition between summer and winter time.
* Add the recent 'humidity' parameter to the monthly and annual reports
* Adding a command to view the weather forecasts 
* Calculation of the number of sunshine hours per month.
* Improvement of the reports to include the concerned city and add the humidity rate for the monthly record, as well as the number of sunshine hours per month.
