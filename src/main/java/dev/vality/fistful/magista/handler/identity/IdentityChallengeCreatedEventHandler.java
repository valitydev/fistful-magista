package dev.vality.fistful.magista.handler.identity;

import dev.vality.fistful.identity.Challenge;
import dev.vality.fistful.identity.ChallengeChange;
import dev.vality.fistful.identity.TimestampedChange;
import dev.vality.fistful.magista.dao.IdentityDao;
import dev.vality.fistful.magista.domain.enums.ChallengeEventType;
import dev.vality.fistful.magista.domain.enums.ChallengeStatus;
import dev.vality.fistful.magista.domain.tables.pojos.ChallengeData;
import dev.vality.fistful.magista.exception.DaoException;
import dev.vality.fistful.magista.exception.StorageException;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdentityChallengeCreatedEventHandler implements IdentityEventHandler {

    private final IdentityDao identityDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetIdentityChallenge()
                && change.getChange().getIdentityChallenge().getPayload().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Trying to handle IdentityChallengeCreated: eventId={}, identityId={}", event.getEventId(),
                    event.getSourceId());
            ChallengeChange challengeChange = change.getChange().getIdentityChallenge();
            Challenge challenge = challengeChange.getPayload().getCreated();
            LocalDateTime eventOccurredAt = TypeUtil.stringToLocalDateTime(change.getOccuredAt());

            ChallengeData challengeData = new ChallengeData();
            challengeData.setEventId(event.getEventId());
            challengeData.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            challengeData.setEventOccurredAt(eventOccurredAt);
            challengeData.setEventType(ChallengeEventType.CHALLENGE_CREATED);
            challengeData.setCreatedAt(eventOccurredAt);
            challengeData.setIdentityId(event.getSourceId());
            challengeData.setChallengeId(challengeChange.getId());
            challengeData.setChallengeClassId(challenge.getCls());
            challengeData.setChallengeStatus(ChallengeStatus.pending);

            Long id = identityDao.save(challengeData);

            log.info("IdentityChallengeCreated has {} been saved: eventId={}, identityId={}",
                    id == null ? "NOT" : "", event.getEventId(), event.getSourceId());
        } catch (DaoException ex) {
            throw new StorageException(ex);
        }
    }

}
