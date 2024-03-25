from portfolio.portfolio_management import model_portfolio

def main():
    portfolio_df = model_portfolio()
    my_portfolio = portfolio_df[portfolio_df['invest'] == 'Y']['종목코드']
    my_portfolio.to_excel('my_portfolio.xlsx', index=False)
    print("Portfolio update complete.")

if __name__ == "__main__":
    main()