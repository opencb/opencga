package org.opencb.opencga.core.common;

import org.passay.CharacterData;
import org.passay.*;

import java.util.ArrayList;
import java.util.List;

public class PasswordUtils {

    public static final int MIN_STRONG_PASSWORD_LENGTH = 8;
    public static final int DEFAULT_PASSWORD_LENGTH = 10;
    public static final int DEFAULT_SALT_LENGTH = 32;
    public static final String PASSWORD_REQUIREMENT = "Password must contain at least " + MIN_STRONG_PASSWORD_LENGTH
            + " characters, including at least one uppercase letter, one lowercase letter, one digit and one special character.";

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
        return getStrongRandomPassword(DEFAULT_PASSWORD_LENGTH);
    }

    public static String getStrongRandomSalt() {
        return getStrongRandomPassword(DEFAULT_SALT_LENGTH);
    }

    public static String getStrongRandomPassword(int length) {
        CharacterRule upper = new CharacterRule(EnglishCharacterData.UpperCase);
        CharacterRule lower = new CharacterRule(EnglishCharacterData.LowerCase);
        CharacterRule digits = new CharacterRule(EnglishCharacterData.Digit);

        PasswordGenerator passwordGenerator = new PasswordGenerator();
        return passwordGenerator.generatePassword(length, upper, lower, digits, SPECIAL_CHARACTER_RULE);
    }

    public static boolean isStrongPassword(String password) {
        List<Rule> rules = new ArrayList<>();
        //Rule 1: Password length should be in between
        //MIN_STRONG_PASSWORD_LENGTH and 100 characters
        rules.add(new LengthRule(MIN_STRONG_PASSWORD_LENGTH, 100));
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
