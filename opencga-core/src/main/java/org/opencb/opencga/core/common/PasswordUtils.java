package org.opencb.opencga.core.common;

import org.passay.*;

import java.util.ArrayList;
import java.util.List;

public class PasswordUtils {

    private final static CharacterRule SPECIAL_CHARACTER_RULE = new CharacterRule(new CharacterData() {
        @Override
        public String getErrorCode() {
            return "INSUFFICIENT_SPECIAL";
        }

        @Override
        public String getCharacters() {
            return "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~¡";
        }
    });

    public static String getStrongRandomPassword() {
        return getStrongRandomPassword(10);
    }

    public static String getStrongRandomPassword(int length) {
        CharacterRule upper = new CharacterRule(EnglishCharacterData.UpperCase);
        CharacterRule lower = new CharacterRule(EnglishCharacterData.LowerCase);
        CharacterRule digits = new CharacterRule(EnglishCharacterData.Digit);

        PasswordGenerator passwordGenerator = new PasswordGenerator();
        return passwordGenerator.generatePassword(length, upper, lower, digits, SPECIAL_CHARACTER_RULE);
    }

    public static boolean isStrongPassword(String password) {
        return isStrongPassword(password, 8);
    }

    public static boolean isStrongPassword(String password, int minLength) {
        List<Rule> rules = new ArrayList<>();
        //Rule 1: Password length should be in between
        //minLength and 100 characters
        rules.add(new LengthRule(minLength, 100));
        //Rule 2: No whitespace allowed
        rules.add(new WhitespaceRule());
        //Rule 3.a: At least one Upper-case character
        rules.add(new CharacterRule(EnglishCharacterData.UpperCase));
        //Rule 3.b: At least one Lower-case character
        rules.add(new CharacterRule(EnglishCharacterData.LowerCase));
        //Rule 3.c: At least one digit
        rules.add(new CharacterRule(EnglishCharacterData.Digit));
        //Rule 3.d: At least one special character
        rules.add(SPECIAL_CHARACTER_RULE);

        PasswordValidator validator = new PasswordValidator(rules);
        PasswordData passwordData = new PasswordData(password);
        RuleResult result = validator.validate(passwordData);

        return result.isValid();
    }
}
