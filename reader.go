func (r *Reader) CommitMessages(ctx context.Context, msgs ...Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Existing implementation of CommitMessages follows...
	return r.commitMessages(ctx, msgs)
}