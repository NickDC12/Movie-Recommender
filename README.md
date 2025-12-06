# HybridReel Movie Recommendation System

A hybrid movie recommendation web application that combines collaborative filtering and content-based filtering to provide personalized movie recommendations.

## Features

- **Hybrid Recommendation Engine**: Combines collaborative filtering (70%) with content-based filtering (30%) for accurate recommendations
- **User Profiles**: Registered users can save ratings and view their profile statistics
- **Guest Mode**: Try the system with pre-loaded demo profiles
- **Interactive Rating System**: Rate movies with 0.5-5.0 star increments
- **Search Functionality**: Find movies quickly with autocomplete search
- **Recommendation Explanations**: See why movies were recommended with detailed score breakdowns
- **Similar Movies**: Find movies similar to ones you enjoy

## Technologies Used

- **Backend**: Python, Flask, SQLite
- **Machine Learning**: scikit-learn, Surprise (scikit-surprise)
- **Frontend**: HTML, CSS, JavaScript, Bootstrap 5
- **Database**: SQLite with pandas integration
- **Data**: MovieLens dataset

## Prerequisites

- Python 3.10+
- pip package manager

## Installation

1. **Clone the repository**
```bash
git clone <your-repo-url>
cd hybridreel
```

2. **Create a virtual environment (optional but recommended)**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Set up the database**
```bash
python src/data_loader.py
```

5. **Run tests (optional)**
```bash
python tests.py
```

## Usage

1. **Start the Flask application**
```bash
python src/app.py
```

2. **Open your browser and navigate to**
```
http://localhost:5000
```

3. **Choose an option:**
   - **Sign up**: Create a new account
   - **Login**: Access your existing account
   - **Browse as Guest**: Try demo profiles

4. **Start rating movies** to build your profile and get personalized recommendations!

---

Link to Website: https://ncolet.pythonanywhere.com/
