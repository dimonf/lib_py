"""
For tagged transactions, replace account with provided.

base code     : divert_account.py pluging by Martin Blais
what modified : - check for type of account for effected transaction is removed (i.e any account is allowed)
                - only postings with meta 'divert' == True are affected

scenario      : 2 files maintained, 1st - with 'general' payments records,
second - with details for particulars job/project, such as this:

general_file:
  ;entry in 'general' file reflects funds withdrawal
  2017-01-28 * "advance paid" "subcontractor_a" #building
    Assets:Bank                       -500 EUR
    Liabilities:Proprietor             500 EUR
       divert: True

project_file:
  ;project file has posting which agreed costs
  plugin "replace_account "['building', 'Liabilities:Subcontractor_a']"
  include <general_file>

  2017-02-15 * "finished 1st stage of thermal insulation" "subcontractor_a"
    Liabilities:Subcontractor_a        -380 EUR
    Expenses:House_maintenance          380 EUR

If we query general_file, then all payments are treated as withdrawal on cash basis. If project_file is queried, transactions from both files combined:
    - balance due to the subcontractor is maintained
    - costs are accounted for on accrual basis
"""
__copyright__ = "Copyright (C) Dmitri Kourbatsky"
__license__ = "GNU GPLv2"

from beancount.core.data import Transaction
from beancount.parser import options


__plugins__ = ('replace_account',)


def replace_account(entries, options_map, config_str):
    """Divert expenses.

    Explicit price entries are simply maintained in the output list. Prices from
    postings with costs or with prices from Transaction entries are synthesized
    as new Price entries in the list of entries output.

    Args:
      entries: A list of directives. We're interested only in the Transaction instances.
      options_map: A parser options dict.
      config_str: A configuration string, which is intended to be a list of two strings,
        a tag, and an account to replace expenses with.
    Returns:
      A modified list of entries.
    """
    # pylint: disable=eval-used
    config_obj = eval(config_str, {}, {})
    if not isinstance(config_obj, dict):
        raise RuntimeError("Invalid plugin configuration: should be a single dict.")
    tag = config_obj['tag']
    replacement_account = config_obj['account']


    new_entries = []
    errors = []
    for entry in entries:
        if isinstance(entry, Transaction) and tag in entry.tags:
            entry = replace_acc(entry, replacement_account)
        new_entries.append(entry)

    return new_entries, errors


def replace_acc(entry, replacement_account):
    """Replace the Expenses accounts from the entry.

    Args:
      entry: A Transaction directive.
      replacement_account: A string, the account to use for replacement.
    Returns:
      A possibly entry directive.
    """
    new_postings = []
    for posting in entry.postings:
        if posting.meta.get('divert', False):
            posting = posting._replace(account=replacement_account,
                                       meta={'diverted_account': posting.account})
        new_postings.append(posting)
    return entry._replace(postings=new_postings)
