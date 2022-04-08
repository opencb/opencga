package org.opencb.opencga.core.common;

import org.passay.*;

import java.util.ArrayList;
import java.util.List;

public class PasswordUtils {

    public static void main(String[] args) {

        System.out.println("Las passwords random cumplen con todos los requisitos " + isStrongPassword(getStrongRandomPassword()));
    }

    public static String getStrongRandomPassword() {
        CharacterRule upper = new CharacterRule(EnglishCharacterData.UpperCase);
        CharacterRule lower = new CharacterRule(EnglishCharacterData.LowerCase);
        CharacterRule digits = new CharacterRule(EnglishCharacterData.Digit);
        CharacterRule special = new CharacterRule(EnglishCharacterData.Special);

        PasswordGenerator passwordGenerator = new PasswordGenerator();
        String password = passwordGenerator.generatePassword(10, upper, lower, digits, special);
        return password;
    }

    public static boolean isStrongPassword(String password) {
        List<Rule> rules = new ArrayList<>();
        //Rule 1: Password length should be in between
        //8 and 16 characters
        rules.add(new LengthRule(8, 100));
        //Rule 2: No whitespace allowed
        rules.add(new WhitespaceRule());
        //Rule 3.a: At least one Upper-case character
        rules.add(new CharacterRule(EnglishCharacterData.UpperCase, 1));
        //Rule 3.b: At least one Lower-case character
        rules.add(new CharacterRule(EnglishCharacterData.LowerCase, 1));
        //Rule 3.c: At least one digit
        rules.add(new CharacterRule(EnglishCharacterData.Digit, 1));
        //Rule 3.d: At least one special character
        rules.add(new CharacterRule(EnglishCharacterData.Special, 1));

        PasswordValidator validator = new PasswordValidator(rules);
        PasswordData passwordData = new PasswordData(password);
        RuleResult result = validator.validate(passwordData);

        if (!result.isValid()) {
            return false;
        }
        return true;
    }
}
