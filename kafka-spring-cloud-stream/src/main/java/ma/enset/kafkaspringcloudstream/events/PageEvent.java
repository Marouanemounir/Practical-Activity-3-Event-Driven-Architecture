package ma.enset.kafkaspringcloudstream.events;

import java.util.Date;

/**
 * Événement métier représentant la visite d'une page web.
 *
 * @param name     nom de la page visitée
 * @param user     utilisateur à l'origine de la visite
 * @param date     date de la visite
 * @param duration durée de la visite en millisecondes
 */
public record PageEvent(String name, String user, Date date, long duration) {
}
