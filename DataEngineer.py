!pip install optbinning
from optbinning import OptimalBinning
import warnings
warnings.filterwarnings("ignore", message="'force_all_finite' was renamed to 'ensure_all_finite'")
warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value encountered in log")

class FeatureEngineer:
    def __init__(self):
        self.binners = {}  # trained binners
        self.numeric_cols = None  # numeric columns for imputation

    def fit(self, df_train: pd.DataFrame, target: pd.Series):
        self.df = df_train.copy()

        # Save numeric columns for NaN imputation
        self.numeric_cols = self.df.select_dtypes(include='number').columns

        # Numeric imputation
        self.df[self.numeric_cols] = self.df[self.numeric_cols].fillna(-99999)

        # Remove constant columns
        self._remove_constant_features()

        # Apply Optimal Binning to categorical variables
        self._fit_optimal_binning(target)

        return self

    def transform(self, df_new: pd.DataFrame):
        df_new = df_new.copy()

        # Numeric imputation same as training
        if self.numeric_cols is not None:
            df_new[self.numeric_cols] = df_new[self.numeric_cols].fillna(-99999)

        # Remove constant columns that were removed during training
        train_cols = set(self.df.columns)
        new_cols = set(df_new.columns)
        cols_to_drop = new_cols - train_cols
        df_new.drop(columns=list(cols_to_drop), errors='ignore', inplace=True)

        # Apply Optimal Binning transformations to categorical variables
        for col, optb in self.binners.items():
            if col in df_new.columns:
                df_new[col] = optb.transform(df_new[col].values, metric="woe")

        return df_new

    def _remove_constant_features(self):
        # Remove constant categorical and numeric columns from training data
        const_cat_cols = [c for c in self.df.select_dtypes(include='object').columns
                          if self.df[c].nunique(dropna=True) <= 1]
        all_null_cat_cols = [c for c in self.df.select_dtypes(include='object').columns
                             if self.df[c].isna().all()]
        const_cat_cols = list(set(const_cat_cols + all_null_cat_cols))

        const_num_cols = [c for c in self.df.select_dtypes(include='number').columns
                          if self.df[c].var(numeric_only=True) == 0]

        cols_to_drop = const_cat_cols + const_num_cols
        self.df.drop(columns=cols_to_drop, inplace=True)

    def _fit_optimal_binning(self, target: pd.Series, solver="mip"):
        cat_cols = self.df.select_dtypes(include="object").columns

        for col in cat_cols:
            if self.df[col].nunique() < 2:
                continue
            optb = OptimalBinning(name=col, dtype="categorical", solver=solver)
            try:
                optb.fit(self.df[col].values, target.values)
                self.binners[col] = optb
                self.df[col] = optb.transform(self.df[col].values, metric="woe")
            except ValueError as e:
                self.df.drop(columns=[col], inplace=True)