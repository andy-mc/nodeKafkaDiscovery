class CooperativeStickyAssignor {
  constructor({ cluster, logger }) {
    this.cluster = cluster;
    this.logger = logger.namespace('CooperativeStickyAssignor');
  }

  // This function should return a unique name for the assignor
  static get name() {
    return 'CooperativeSticky';
  }

  // This function is called when the assignor is registered in the Kafka consumer
  onAssignment(assignments) {
    this.assignments = assignments;
  }

  // This function is called when a rebalance is triggered
  async assign({ members, topics }) {
    const sortedMembers = members.map(({ memberId }) => memberId).sort();
    const sortedTopics = topics.sort();

    const assignments = {};
    for (const memberId of sortedMembers) {
      assignments[memberId] = [];
    }

    // Distribute partitions using a round-robin strategy
    let currentMemberIndex = 0;
    for (const topic of sortedTopics) {
      const partitions = await this.cluster.fetchTopicOffsets(topic);
      for (const partition of partitions) {
        assignments[sortedMembers[currentMemberIndex]].push({ topic, partition: partition.partition });
        currentMemberIndex = (currentMemberIndex + 1) % sortedMembers.length;
      }
    }

    return Object.entries(assignments).map(([memberId, memberAssignments]) => ({
      memberId,
      memberAssignments,
    }));
  }
}

module.exports = CooperativeStickyAssignor;
