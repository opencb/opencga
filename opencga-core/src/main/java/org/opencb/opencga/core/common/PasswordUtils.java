package org.opencb.opencga.core.common;

import org.passay.*;

import java.util.ArrayList;
import java.util.List;

public class PasswordUtils {

    public static String getStrongRandomPassword() {
        return getStrongRandomPassword(10);
    }

    public static String getStrongRandomPassword(int length) {
        CharacterRule upper = new CharacterRule(EnglishCharacterData.UpperCase);
        CharacterRule lower = new CharacterRule(EnglishCharacterData.LowerCase);
        CharacterRule digits = new CharacterRule(EnglishCharacterData.Digit);
        CharacterRule special = new CharacterRule(EnglishCharacterData.Special);

        PasswordGenerator passwordGenerator = new PasswordGenerator();
        return passwordGenerator.generatePassword(length, upper, lower, digits, special);
    }

    public static boolean isStrongPassword(String password) {
        return isStrongPassword(password, 8);
    }

    public static boolean isStrongPassword(String password, int minLength) {
        List<Rule> rules = new ArrayList<>();
        //Rule 1: Password length should be in between
        //8 and 100 characters
        rules.add(new LengthRule(minLength, 100));
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

        return result.isValid();
    }
}
