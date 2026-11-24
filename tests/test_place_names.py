from app.utils.place_names import lga_name, state_name, ward_name


def test_dhis2_names_are_cleaned_for_display():
    assert state_name('kw Kwara State') == 'Kwara'
    assert state_name('Kwara State State') == 'Kwara'
    assert state_name('Kwara') == 'Kwara'
    assert lga_name('kw Moro Local Government Area') == 'Moro'
    assert lga_name('Ilorin West') == 'Ilorin West'
    assert ward_name('kw Gwa0be 2 Ward') == 'Gwa0be 2'
    assert ward_name('Owode/Gbogun') == 'Owode/Gbogun'
