"""Approximate ward matching: best pairs first, numbers must agree, one data ward per map ward."""
import pandas as pd

from app.tpr.utils import approximate_ward_pairs
from app.utils.ward_matcher import normalize_ward_name


def _frame(rows):
    frame = pd.DataFrame(rows, columns=['WardName', 'LGA'])
    frame['WardName_norm'] = frame['WardName'].map(normalize_ward_name)
    return frame


def test_numbers_and_letters_must_agree():
    map_wards = _frame([('Akanbi 2', 'Ilorin South'), ('Essa B', 'Offa'), ('Pategi Ii', 'Pategi')])
    data_wards = _frame([('akanbi 4', 'kw Ilorin South Local Government Area'),
                         ('essa c', 'kw Offa Local Government Area'),
                         ('patigi 1', 'kw Pategi Local Government Area')])
    assert approximate_ward_pairs(map_wards, data_wards) == {}


def test_extra_lga_or_state_word_still_matches():
    map_wards = _frame([('Owu', 'Isin'), ('Ikotun-Kwara', 'Oyun'), ('Pategi Ii', 'Pategi')])
    data_wards = _frame([('owu isin', 'kw Isin Local Government Area'),
                         ('ikotun', 'kw Oyun Local Government Area'),
                         ('patigi 2', 'kw Pategi Local Government Area')])
    assert approximate_ward_pairs(map_wards, data_wards) == {0: 0, 1: 1, 2: 2}


def test_each_data_ward_is_used_once_and_best_pair_wins():
    map_wards = _frame([('Idofin 1', 'Oke Ero'), ('Idofian 1', 'Oke Ero')])
    data_wards = _frame([('idofian 1', 'kw Oke-Ero Local Government Area')])
    assert approximate_ward_pairs(map_wards, data_wards) == {1: 0}


def test_spelling_variants_in_same_lga_match_but_not_across_lgas():
    map_wards = _frame([('Igbomma', 'Oyun'), ('Agwijin', 'Ifelodun')])
    data_wards = _frame([('igbonna', 'kw Oyun Local Government Area'),
                         ('agunjin', 'kw Asa Local Government Area')])
    assert approximate_ward_pairs(map_wards, data_wards) == {0: 0}


def test_number_on_one_side_only_matches_when_unique():
    map_wards = _frame([('Omupo', 'Ifelodun'), ('Yashikira 1', 'Baruten'), ('Yashikira 2', 'Baruten')])
    data_wards = _frame([('omupo 1', 'kw Ifelodun Local Government Area'),
                         ('yashikira', 'kw Baruten Local Government Area')])
    assert approximate_ward_pairs(map_wards, data_wards) == {0: 0}
