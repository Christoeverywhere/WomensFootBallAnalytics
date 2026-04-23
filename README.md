# WomensFootBallAnalytics

A machine learning project focused on **women’s football injury risk prediction** using player workload, recovery metrics, and performance data.  
The system analyzes key factors such as training intensity, workload-recovery balance, and player condition to help identify potential injury risks and support better decision-making for coaches and analysts.

---

## Overview

This project aims to use **data analytics + machine learning** to study injury risk in women’s football.  
By collecting and processing football-related data, the model predicts whether a player may be at a higher risk of injury based on measurable workload and recovery indicators.

The project is divided into two major parts:

- **DataScraper** → Collects and prepares football-related data
- **Injury_Prediction** → Builds and evaluates the machine learning model for injury prediction

---

## Objectives

- Analyze player workload and recovery patterns
- Identify important injury risk indicators
- Build a predictive model using **XGBoost**
- Support injury prevention and performance management
- Provide actionable insights for coaches, analysts, and sports scientists

---

## Project Structure

```bash
WomensFootBallAnalytics/
│
├── DataScraper/           # Scripts for collecting / scraping football-related data
├── Injury_Prediction/     # ML model, preprocessing, training, and evaluation
└── README.md              # Project documentation
