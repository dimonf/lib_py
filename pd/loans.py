#import click
from decimal import Decimal, getcontext
import locale
import pandas as pd


class Amortization(object):
    #stolen from https://github.com/mandeep/Amortization

    def __init__(self, present_value, interest_rate, years, annual_payments, name="Loan"):
        """Locale is used to format currency symbols to amounts. The precision
        for Decimal is set to 50 to eliminate rounding issues."""
        locale.setlocale(locale.LC_ALL, '')
        getcontext().prec = 50

        self.rounding = Decimal(10) ** -2
        self.present_value = Decimal(present_value)
        self.years = Decimal(years)
        self.annual_payments = Decimal(annual_payments)
        self.interest_rate = Decimal(interest_rate) / 100 / self.annual_payments
        self.periods = int(self.years * self.annual_payments)
        self.name = name

    def monthly_payment(self):
        """Calculates the monthly payment of the given loan using the annuity
        formula."""
        amount = ((self.interest_rate * self.present_value *
                   pow(1 + self.interest_rate, self.periods)) /
                  (pow(1 + self.interest_rate, self.periods) - 1))
        return amount

    def beginning_balances(self):
        """Calculates the beginning balance of the loan after each payment.
        The beginning balance after the first payment consists of the
        previous balance minus the principal portion of the monthly payment."""
        balances = []
        beginning_balance = self.present_value
        for i in range(self.periods):
            balances.append(beginning_balance)
            beginning_balance -= (self.monthly_payment() - (beginning_balance * self.interest_rate))
        return balances

    def monthly_payments(self):
        """Calculates the monthly payment for each beginning balance."""
        number_of_monthly_payments = []
        for i in range(self.periods):
            if self.monthly_payment() < self.beginning_balances()[i]:
                number_of_monthly_payments.append(self.monthly_payment())
            else:
                number_of_monthly_payments.append(self.beginning_balances()[i])
        return number_of_monthly_payments

    def principal_payments(self):
        """Calculates the principal portion of each monthly payment."""
        principal_payments_balances = []
        for i in range(self.periods):
            if self.monthly_payment() - self.interest_payments()[i] < self.beginning_balances()[i]:
                principal_payments_balances.append(self.monthly_payment() - self.interest_payments()[i])
            else:
                principal_payments_balances.append(principal_payments_balances[i-1])
        return principal_payments_balances

    def interest_payments(self):
        """Calculates the interest portion of each monthly payment."""
        return [(balance * self.interest_rate) for balance in self.beginning_balances()]

    def ending_balances(self):
        """Calculates the ending balance after each monthly payment by subtracting
        the prinicpal portion of the monthly payment from the beginning balance."""
        return [(balance - payment) for balance, payment in zip(
                 self.beginning_balances(), self.monthly_payments())]

    def cumulative_interest(self):
        """Calculates the cumulative interest after each monthly payment."""
        cumulative = []
        cumulative_interest_payment = 0
        for i in self.interest_payments():
            cumulative_interest_payment += i
            cumulative.append(cumulative_interest_payment)
        return cumulative

    def amortization_schedule(self):
        """Outputs an amortization schedule in a Pandas dataframe with columns for the
        number of payments, beginning balance, monthly payment, principal payment,
        interest payment, ending balance, and cumulative interest."""
        schedule_beginning_balances = [locale.currency(Decimal(b).quantize(self.rounding),
                                       grouping=True) for b in self.beginning_balances()]

        schedule_monthly_payments = [locale.currency(Decimal(m).quantize(self.rounding),
                                     grouping=True) for m in self.monthly_payments()]

        schedule_principal_payments = [locale.currency(Decimal(p).quantize(self.rounding),
                                       grouping=True) for p in self.principal_payments()]

        schedule_interest_payments = [locale.currency(Decimal(i).quantize(self.rounding),
                                      grouping=True) for i in self.interest_payments()]

        schedule_ending_balances = [locale.currency(Decimal(e).quantize(self.rounding),
                                    grouping=True) for e in self.ending_balances()]

        schedule_cumulative_interest = [locale.currency(Decimal(c).quantize(self.rounding),
                                        grouping=True) for c in self.cumulative_interest()]

        df = pd.DataFrame.from_items([('Beginning Balance', schedule_beginning_balances),
                                      ('Monthly Payment', schedule_monthly_payments),
                                      ('Principal', schedule_principal_payments),
                                      ('Interest', schedule_interest_payments),
                                      ('Ending Balance', schedule_ending_balances),
                                      ('Cumulative Interest', schedule_cumulative_interest)])
        df.index = df.index + 1
        pd.set_option('display.max_rows', len(df))
        pd.set_option('display.width', 1080)
        return df
